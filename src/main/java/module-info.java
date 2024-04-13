module com.tugalsan.api.sql.tbl {
    requires java.sql;
    requires com.tugalsan.api.list;
    requires com.tugalsan.api.union;
    requires com.tugalsan.api.runnable;
    requires com.tugalsan.api.log;
    requires com.tugalsan.api.stream;
    requires com.tugalsan.api.string;
    requires com.tugalsan.api.sql.col.typed;
    requires com.tugalsan.api.sql.conn;
    requires com.tugalsan.api.sql.sanitize;
    requires com.tugalsan.api.sql.select;
    requires com.tugalsan.api.sql.update;
    requires com.tugalsan.api.sql.resultset;
    exports com.tugalsan.api.sql.tbl.server;
}
