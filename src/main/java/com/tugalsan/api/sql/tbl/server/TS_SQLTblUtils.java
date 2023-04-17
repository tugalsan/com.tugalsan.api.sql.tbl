package com.tugalsan.api.sql.tbl.server;

import java.util.*;
import com.tugalsan.api.list.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.pack.client.*;
import com.tugalsan.api.string.client.*;
import com.tugalsan.api.sql.col.typed.client.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.sql.select.server.*;
import com.tugalsan.api.sql.update.server.*;
import com.tugalsan.api.unsafe.client.*;

public class TS_SQLTblUtils {

    final private static TS_Log d = TS_Log.of(TS_SQLTblUtils.class);

    public static void setIndexes(TS_SQLConnAnchor anchor, String tn, List<String> searchableColNames) {
        var curIndexes = getIndexes(anchor, tn);
        curIndexes.stream()
                .filter(nm -> searchableColNames.stream().filter(nm2 -> Objects.equals(nm, nm2)).findAny().isEmpty())
                .forEach(nm -> removeIndex(anchor, tn, nm));
        searchableColNames.stream()
                .filter(nm -> curIndexes.stream().filter(nm2 -> Objects.equals(nm, nm2)).findAny().isEmpty())
                .forEach(nm -> addIndex(anchor, tn, nm));
    }

    public static void removeIndexesAll(TS_SQLConnAnchor anchor, CharSequence tableName) {
        removeIndexes(anchor, tableName, getIndexes(anchor, tableName));
    }

    public static void removeIndexes(TS_SQLConnAnchor anchor, CharSequence tableName, List<String> indexNames) {
        indexNames.forEach(nm -> removeIndex(anchor, tableName, nm));
    }

    public static void removeIndexes(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence... indexNames) {
        TGS_ListUtils.of(indexNames).forEach(nm -> removeIndex(anchor, tableName, nm));
    }

    public static TS_SQLConnStmtUpdateResult removeIndex(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence indexName) {
        d.ci("removeIndex", tableName, indexName);
        if (Objects.equals(indexName, "PRIMARY")) {
            d.ci("addIndex", "cannot remove index", indexName);
            return TS_SQLConnStmtUpdateResult.of(0, null);
        }
        var sql = "DROP INDEX " + indexName + " ON " + tableName;
        return TS_SQLUpdateStmtUtils.update(anchor, sql);
    }

    public static List<String> getIndexes(TS_SQLConnAnchor anchor, CharSequence tableName) {
        List<String> cns = TGS_ListUtils.of();
        var sql = "SHOW INDEX FROM " + tableName;
        TS_SQLSelectStmtUtils.select(anchor, sql, rs -> rs.walkRows(null, ri -> {
            cns.add(rs.str.get(2));
        }));
        return cns;
    }

    public static void addIndex(TS_SQLConnAnchor anchor, CharSequence tableName, List<String> idxColNames) {
        idxColNames.forEach(nm -> addIndex(anchor, tableName, nm));
    }

    public static void addIndex(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence... idxColNames) {
        TGS_ListUtils.of(idxColNames).forEach(nm -> addIndex(anchor, tableName, nm));
    }

    public static void addIndex(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence idxColName) {
        d.ci("addIndex", tableName, idxColName);
        if (getIndexes(anchor, tableName).stream().filter(nm -> Objects.equals(nm, idxColName)).findAny().isPresent()) {
            d.ce("addIndex", "index already exists", idxColName);
            return;
        }
        var sql = TGS_StringUtils.concat("ALTER TABLE ", tableName, " ADD INDEX(", idxColName, ")");
        TS_SQLUpdateStmtUtils.update(anchor, sql);
    }

    public static boolean rename(TS_SQLConnAnchor anchor, CharSequence oldTableName, CharSequence newTableName) {
        TS_SQLSanitizeUtils.sanitize(oldTableName);
        TS_SQLSanitizeUtils.sanitize(newTableName);
        var sql = TGS_StringUtils.concat("ALTER TABLE ", oldTableName, " RENAME TO ", newTableName);
        return TS_SQLUpdateStmtUtils.update(anchor, sql).affectedRowCount == 1;
    }

    public static List<String> names(TS_SQLConnAnchor anchor) {
        var dbName = anchor.config.dbName;
        TS_SQLSanitizeUtils.sanitize(dbName);
        TGS_Pack1<List<String>> pack = new TGS_Pack1();
        var sql = TGS_StringUtils.concat("SELECT TABLE_NAME FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", dbName, "' ORDER BY table_schema");
        TS_SQLSelectStmtUtils.select(anchor, sql, rs -> pack.value0 = rs.strArr.get("TABLE_NAME"));
        return pack.value0;
    }

    public static boolean exists(TS_SQLConnAnchor anchor, CharSequence tableName) {
        var dbName = anchor.config.dbName;
        TS_SQLSanitizeUtils.sanitize(dbName);
        TS_SQLSanitizeUtils.sanitize(tableName);
        TGS_Pack1<Long> pack = new TGS_Pack1();
        var sql = TGS_StringUtils.concat("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND TABLE_SCHEMA = ? LIMIT 1");
        TS_SQLSelectStmtUtils.select(anchor, sql, ps -> {
            TGS_UnSafe.run(() -> {
                ps.setString(1, tableName.toString());
                ps.setString(2, dbName);
            });
        }, rs -> pack.value0 = rs.lng.get(0, 0));
        return pack.value0 != 0L;
    }

    public static boolean createIfNotExists(TS_SQLConnAnchor anchor, boolean onMemory, CharSequence tableName, List<TGS_SQLColTyped> columns) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        TS_SQLSanitizeUtils.sanitize(columns);
        var columnsList = new StringBuilder();
        columns.forEach(ct -> {
            columnsList.append(ct.toString());
            columnsList.append(" ");
            columnsList.append(TS_SQLConnColUtils.creationType(anchor, ct));
            columnsList.append(", ");
        });
        var columnNameId = columns.get(0).toString();
        var sql = TGS_StringUtils.concat("CREATE TABLE IF NOT EXISTS ", tableName, " (", columnsList.toString(), " PRIMARY KEY (", columnNameId, ")) ENGINE=", (onMemory ? "MEMORY" : "InnoDB"), " DEFAULT CHARSET=utf8mb4;");
        TS_SQLUpdateStmtUtils.update(anchor, sql);
        return true;
    }

    public static boolean deleteIfExists(TS_SQLConnAnchor anchor, CharSequence tableName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        var sql = TGS_StringUtils.concat("DROP TABLE IF EXISTS ", tableName);
        TS_SQLUpdateStmtUtils.update(anchor, sql);
        return true;
    }

}
