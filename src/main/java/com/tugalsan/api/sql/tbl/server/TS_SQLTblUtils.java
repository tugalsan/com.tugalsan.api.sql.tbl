package com.tugalsan.api.sql.tbl.server;

import java.util.*;
import com.tugalsan.api.list.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.string.client.*;
import com.tugalsan.api.sql.col.typed.client.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.sql.select.server.*;
import com.tugalsan.api.sql.update.server.*;
import com.tugalsan.api.stream.client.TGS_StreamUtils;
import com.tugalsan.api.union.client.TGS_UnionExcuse;
import com.tugalsan.api.union.client.TGS_UnionExcuseVoid;
import java.sql.SQLException;

public class TS_SQLTblUtils {

    final private static TS_Log d = TS_Log.of(TS_SQLTblUtils.class);

    public static TGS_UnionExcuseVoid setIndexes(TS_SQLConnAnchor anchor, String tn, List<String> searchableColNames) {
        var u_curIndexes = getIndexes(anchor, tn);
        if (u_curIndexes.isExcuse()) {
            return u_curIndexes.toExcuseVoid();
        }
        var curIndexes = u_curIndexes.value();
        curIndexes.stream()
                .filter(nm -> searchableColNames.stream().filter(nm2 -> Objects.equals(nm, nm2)).findAny().isEmpty())
                .forEach(nm -> removeIndex(anchor, tn, nm));
        searchableColNames.stream()
                .filter(nm -> curIndexes.stream().filter(nm2 -> Objects.equals(nm, nm2)).findAny().isEmpty())
                .forEach(nm -> addIndex(anchor, tn, nm));
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuseVoid removeIndexesAll(TS_SQLConnAnchor anchor, CharSequence tableName) {
        var u = getIndexes(anchor, tableName);
        if (u.isExcuse()) {
            return u.toExcuseVoid();
        }
        return removeIndexes(anchor, tableName, u.value());
    }

    public static TGS_UnionExcuseVoid removeIndexes(TS_SQLConnAnchor anchor, CharSequence tableName, List<String> indexNames) {
        for (var nm : indexNames) {
            var u = removeIndex(anchor, tableName, nm);
            if (u.isExcuse()) {
                return u.toExcuseVoid();
            }
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuseVoid removeIndexes(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence... indexNames) {
        return removeIndexes(anchor, tableName, TGS_StreamUtils.toLst(TGS_ListUtils.of(indexNames).stream().map(cs -> cs.toString())));
    }

    public static TGS_UnionExcuse<TS_SQLConnStmtUpdateResult> removeIndex(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence indexName) {
        d.ci("removeIndex", tableName, indexName);
        if (Objects.equals(indexName, "PRIMARY")) {
            return TGS_UnionExcuse.ofExcuse(d.className, "removeIndex", "cannot remove primary");
        }
        var sql = "DROP INDEX " + indexName + " ON " + tableName;
        return TS_SQLUpdateStmtUtils.update(anchor, sql);
    }

    public static TGS_UnionExcuse<List<String>> getIndexes(TS_SQLConnAnchor anchor, CharSequence tableName) {
        List<String> cns = TGS_ListUtils.of();
        var sql = "SHOW INDEX FROM " + tableName;
        var wrap = new Object() {
            TGS_UnionExcuse<String> u_rs_str_get;
        };
        var u_update = TS_SQLSelectStmtUtils.select(anchor, sql, rs -> rs.walkRows(null, ri -> {
            if (wrap.u_rs_str_get != null && wrap.u_rs_str_get.isExcuse()) {
                return;
            }
            wrap.u_rs_str_get = rs.str.get(2);
            if (wrap.u_rs_str_get.isExcuse()) {
                return;
            }
            cns.add(wrap.u_rs_str_get.value());
        }));
        if (wrap.u_rs_str_get != null && wrap.u_rs_str_get.isExcuse()) {
            return u_update.toExcuse();
        }
        if (u_update.isExcuse()) {
            return u_update.toExcuse();
        }
        return TGS_UnionExcuse.of(cns);
    }

    public static TGS_UnionExcuseVoid addIndexes(TS_SQLConnAnchor anchor, CharSequence tableName, List<String> idxColNames) {
        for (var nm : idxColNames) {
            var u = addIndex(anchor, tableName, nm);
            if (u.isExcuse()) {
                return u;
            }
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuseVoid addIndexes(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence... idxColNames) {
        return addIndexes(anchor, tableName, TGS_StreamUtils.toLst(TGS_ListUtils.of(idxColNames).stream().map(cs -> cs.toString())));
    }

    public static TGS_UnionExcuseVoid addIndex(TS_SQLConnAnchor anchor, CharSequence tableName, CharSequence idxColName) {
        d.ci("addIndex", tableName, idxColName);
        var u_getIndexes = getIndexes(anchor, tableName);
        if (u_getIndexes.isExcuse()) {
            return u_getIndexes.toExcuseVoid();
        }
        if (u_getIndexes.value().stream().filter(nm -> Objects.equals(nm, idxColName)).findAny().isPresent()) {
            d.ci("addIndex", "index already exists", idxColName);
            return TGS_UnionExcuseVoid.ofVoid();
        }
        var sql = TGS_StringUtils.concat("ALTER TABLE ", tableName, " ADD INDEX(", idxColName, ")");
        var u_update = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u_update.isExcuse()) {
            return u_update.toExcuseVoid();
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuseVoid rename(TS_SQLConnAnchor anchor, CharSequence oldTableName, CharSequence newTableName) {
        TS_SQLSanitizeUtils.sanitize(oldTableName);
        TS_SQLSanitizeUtils.sanitize(newTableName);
        var sql = TGS_StringUtils.concat("ALTER TABLE ", oldTableName, " RENAME TO ", newTableName);
        var u = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u.isExcuse()) {
            return u.toExcuseVoid();
        }
        if (u.value().affectedRowCount() != 1) {
            return TGS_UnionExcuseVoid.ofExcuse(d.className, "rename", "u.value().affectedRowCount() != 1");
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuse< List<String>> names(TS_SQLConnAnchor anchor) {
        var dbName = anchor.config.dbName;
        TS_SQLSanitizeUtils.sanitize(dbName);
        var sql = TGS_StringUtils.concat("SELECT TABLE_NAME FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='", dbName, "' ORDER BY table_schema");
        var wrap = new Object() {
            TGS_UnionExcuse<List<String>> rs_strArr_get;
        };
        var u_select = TS_SQLSelectStmtUtils.select(anchor, sql, rs -> {
            wrap.rs_strArr_get = rs.strArr.get("TABLE_NAME");
        });
        if (wrap.rs_strArr_get != null && wrap.rs_strArr_get.isExcuse()) {
            return wrap.rs_strArr_get.toExcuse();
        }
        if (u_select.isExcuse()) {
            return u_select.toExcuse();
        }
        return TGS_UnionExcuse.of(wrap.rs_strArr_get.value());
    }

    public static TGS_UnionExcuse<Boolean> exists(TS_SQLConnAnchor anchor, CharSequence tableName) {
        var dbName = anchor.config.dbName;
        TS_SQLSanitizeUtils.sanitize(dbName);
        TS_SQLSanitizeUtils.sanitize(tableName);
        var wrap = new Object() {
            TGS_UnionExcuseVoid u_fill;
            TGS_UnionExcuse<Long> u_rs_lng_get;
        };
        var sql = TGS_StringUtils.concat("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND TABLE_SCHEMA = ? LIMIT 1");
        var u_select = TS_SQLSelectStmtUtils.select(anchor, sql, ps -> {
            try {
                ps.setString(1, tableName.toString());
                ps.setString(2, dbName);
                wrap.u_fill = TGS_UnionExcuseVoid.ofVoid();
            } catch (SQLException ex) {
                wrap.u_fill = TGS_UnionExcuseVoid.ofExcuse(ex);
            }
        }, rs -> wrap.u_rs_lng_get = rs.lng.get(0, 0));
        if (wrap.u_fill != null && wrap.u_fill.isExcuse()) {
            return wrap.u_fill.toExcuse();
        }
        if (wrap.u_rs_lng_get != null && wrap.u_rs_lng_get.isExcuse()) {
            return wrap.u_rs_lng_get.toExcuse();
        }
        if (u_select.isExcuse()) {
            return u_select.toExcuse();
        }
        return TGS_UnionExcuse.of(wrap.u_rs_lng_get.value() != 0L);
    }

    public static TGS_UnionExcuseVoid createIfNotExists(TS_SQLConnAnchor anchor, boolean onMemory, CharSequence tableName, List<TGS_SQLColTyped> columns) {
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
        var u_update = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u_update.isExcuse()) {
            return u_update.toExcuseVoid();
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

    public static TGS_UnionExcuseVoid deleteIfExists(TS_SQLConnAnchor anchor, CharSequence tableName) {
        TS_SQLSanitizeUtils.sanitize(tableName);
        var sql = TGS_StringUtils.concat("DROP TABLE IF EXISTS ", tableName);
        var u_update = TS_SQLUpdateStmtUtils.update(anchor, sql);
        if (u_update.isExcuse()) {
            return u_update.toExcuseVoid();
        }
        return TGS_UnionExcuseVoid.ofVoid();
    }

}
