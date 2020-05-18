-- from https://support.snowflake.net/s/question/0D50Z00009Y9vYaSAJ/regarding-a-regular-expression-search-in-all-schematablescolumns
CREATE OR REPLACE PROCEDURE DATABASE_SEARCH(TABLE_CATALOG TEXT, RESULT_TABLE TEXT, SEARCH_TERM TEXT, SEARCH_TYPE TEXT, EXCEPT_LIST ARRAY)
RETURNS variant
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var create_tab_sql = `CREATE TABLE IF NOT EXISTS IDENTIFIER(:1) (
        DB TEXT, SCHEMA TEXT, TAB TEXT, COL TEXT, SEARCH TEXT, TYPE TEXT, MATCH_RATE FLOAT, TS TIMESTAMP_LTZ
    )`;
    snowflake.execute({ sqlText: create_tab_sql, binds: [RESULT_TABLE] });
    
    var start_time = Date.now();
 
    var meta_sql = `
      SELECT
          'INSERT INTO IDENTIFIER(:2)\n'
          ||'SELECT\n'
          ||'    TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, :3 "SEARCH", :4 "TYPE",\n'
          ||'    DECODE(ORDINAL_POSITION'||LISTAGG(', '||ORDINAL_POSITION||', C'||ORDINAL_POSITION) WITHIN GROUP (ORDER BY ORDINAL_POSITION)||') MATCH_RATE,\n'
          ||'    CURRENT_TIMESTAMP() TS\n'
          ||'FROM IDENTIFIER(:1)\n'
          ||'CROSS JOIN\n(SELECT \n'
          || LISTAGG('    '||DECODE(UPPER(:4), 'REGEXP', 'SUM(REGEXP_COUNT("'||COLUMN_NAME||'", :3))', 'COUNT(CASE WHEN "'||COLUMN_NAME||'" = :3 THEN 1 END)')
                     ||' / NULLIF(COUNT('||NVL2(:5, 'CASE WHEN "'||COLUMN_NAME||'" NOT IN ('||:5||') THEN "'||COLUMN_NAME||'" END', '"'||COLUMN_NAME||'"')||'), 0) C'||ORDINAL_POSITION, ',\n')
             WITHIN GROUP (ORDER BY COLUMN_NAME) || '\n'
          ||'FROM "'||TABLE_CATALOG||'"."'||TABLE_SCHEMA||'"."'||TABLE_NAME||'")\n'
          ||'WHERE (TABLE_CATALOG = '''||TABLE_CATALOG||''' AND TABLE_SCHEMA = '''||TABLE_SCHEMA ||''' AND TABLE_NAME = '''||TABLE_NAME||''')\n'
          ||'  AND CHARACTER_MAXIMUM_LENGTH > 0'
          SEARCH_SQL
      FROM IDENTIFIER(:1)
      WHERE CHARACTER_MAXIMUM_LENGTH > 0
      GROUP BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
    `;
 
    EXCEPT_LIST = Array.isArray(EXCEPT_LIST)
        ? EXCEPT_LIST.map(x => "'" + x.replace("'", "''") + "'").join(", ")
        : null ;
    var COLUMNS_VIEW = '"' + TABLE_CATALOG + '"."INFORMATION_SCHEMA"."COLUMNS"';
    var binds = [COLUMNS_VIEW, RESULT_TABLE, SEARCH_TERM, SEARCH_TYPE, EXCEPT_LIST].map(x => x === undefined ? null : x);
 
    rs = snowflake.execute({ sqlText: meta_sql, binds: binds }); //rs.next(); return rs.getColumnValue(1);
 
    var table_qty = 0;
    var column_qty = 0;
    var error_qty = 0;
    while (rs.next()) {
        var search_sql = rs.getColumnValue(1);
        table_qty++;
        try {
          var insert = snowflake.execute({ sqlText: search_sql, binds: binds });
          while (insert.next()) { column_qty += insert.getColumnValue('number of rows inserted') };
        }
        catch (err) {  error_qty++; }
    }
 
    var elapsed_time = new Date(Date.now() - start_time).toUTCString().substr(17, 9);
    return  "Searched " + TABLE_CATALOG + " for " + SEARCH_TYPE + " '" + SEARCH_TERM + (EXCEPT_LIST === null ? "" : " except (" + EXCEPT_LIST + ")")
           + ", " + error_qty + " errors, stored " + column_qty + " columns from " + table_qty + " tables into " + RESULT_TABLE + " in " + elapsed_time + "; last query: " + search_sql;
$$
;
DROP TABLE IF EXISTS RESULT_TABLE;
CALL DATABASE_SEARCH('GEO_DATA', 'RESULT_TABLE', 'Trousdale', 'KEYWORD', NULL);

SELECT *
FROM RESULT_TABLE
WHERE SCHEMA != 'INFORMATION_SCHEMA'
    AND TAB != 'RESULT_TABLE'

/*
SEARCH_TERM = 'Trousdale'
SEARCH_TYPE = 'KEYWORD'

SEARCH_TERM = '.*Pacific.*'
SEARCH_TYPE = 'REGEXP'
*/

SELECT * FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE
WHERE COUNTY ILIKE '%trousdale%'

SELECT * FROM GEO_DATA.PUBLIC.ZIP_GEODATA_COMPLETE
WHERE STATE_NAME ILIKE '%trousdale%'