package com.back.db;

import lombok.Setter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SimpleDb {
    private final String url;
    private final String username;
    private final String password;
    @Setter
    private boolean devMode = false;

    private final ThreadLocal<Connection> connectionHolder = new ThreadLocal<>();

    public SimpleDb(String host, String user, String pass, String dbName) {
        this.url = "jdbc:mysql://" + host + "/" + dbName + "?serverTimezone=Asia/Seoul";
        this.username = user;
        this.password = pass;
    }

    private void initConnection() {
        try {
            Connection conn = connectionHolder.get();
            if (conn == null || conn.isClosed()) {
                conn = DriverManager.getConnection(url, username, password);
                connectionHolder.set(conn);
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB Connection 획득 실패", e);
        }
    }

    public Sql genSql() {
        initConnection();
        return new Sql(connectionHolder.get(), devMode);
    }

    public void startTransaction() {
        try {
            connectionHolder.get().setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException("setAutoCommit() 예외 발생", e);
        }
    }

    public void commit() {
        try {
            connectionHolder.get().commit();
        } catch (SQLException e) {
            throw new RuntimeException("commit() 예외 발생", e);
        } finally {
            close();
        }
    }

    public void rollback() {
        try {
            connectionHolder.get().rollback();
        } catch (SQLException e) {
            throw new RuntimeException("rollback() 예외 발생", e);
        } finally {
            close();
        }
    }

    public void close() {
        try {
            connectionHolder.get().close();
            connectionHolder.remove();
        } catch (SQLException e) {
            throw new RuntimeException("close() 예외 발생", e);
        }
    }

    public void run(String sql, Object... params) {
        Sql s = genSql();
        s.append(sql, params);
        s.update();
        close();
    }
}
