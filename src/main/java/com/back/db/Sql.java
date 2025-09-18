package com.back.db;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@Slf4j
public class Sql {

    private final Connection conn;
    private final boolean devMode;
    private final StringBuilder sqlBuilder = new StringBuilder();
    private final List<Object> params = new ArrayList<>();

    public Sql(Connection conn, boolean devMode) {
        this.conn = conn;
        this.devMode = devMode;
    }

    public Sql append(String sqlPart, Object... args) {
        if (!sqlBuilder.isEmpty()) sqlBuilder.append(" ");
        sqlBuilder.append(sqlPart);
        params.addAll(Arrays.asList(args));
        return this;
    }

    public Sql appendIn(String sqlPart, Object... args) {
        String placeholders = String.join(",", Collections.nCopies(args.length, "?"));
        sqlPart = sqlPart.replace("?", placeholders);
        return append(sqlPart, args);
    }

    private void setParams(PreparedStatement stmt) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            stmt.setObject(i + 1, params.get(i));
        }
    }

    private void loggingSql() {
        if (devMode) log.info("SQL: {} / Params: {}", sqlBuilder, params);
    }

    private PreparedStatement createPreparedStatement(String sql, boolean returnGeneratedKeys) throws SQLException {
        return returnGeneratedKeys
                ? conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
                : conn.prepareStatement(sql);
    }

    private <T> T runStatement(boolean returnKeys, SqlFunction<PreparedStatement, T> action) {
        try (PreparedStatement stmt = createPreparedStatement(sqlBuilder.toString(), returnKeys)) {
            setParams(stmt);
            loggingSql();
            return action.apply(stmt);
        } catch (SQLException e) {
            throw new RuntimeException("Sql 실행 중 오류", e);
        } finally {
            clear();
        }
    }

    public long insert() {
        return runStatement(true, stmt -> {
            stmt.executeUpdate();
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        });
    }

    public int update() {
        return runStatement(false, PreparedStatement::executeUpdate);
    }

    public int delete() {
        return runStatement(false, PreparedStatement::executeUpdate);
    }

    public List<Map<String, Object>> selectRows() {
        return runStatement(false, stmt -> {
            try (ResultSet rs = stmt.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        row.put(meta.getColumnName(i), rs.getObject(i));
                    }
                    rows.add(row);
                }
                return rows;
            }
        });
    }

    public Map<String, Object> selectRow() {
        List<Map<String, Object>> rows = selectRows();
        return rows.isEmpty() ? null : rows.getFirst();
    }

    public <T> List<T> selectRows(Class<T> clazz) {
        return selectRows().stream().map(row -> mapToObject(row, clazz)).toList();
    }

    public <T> T selectRow(Class<T> clazz) {
        Map<String, Object> row = selectRow();
        return row == null ? null : mapToObject(row, clazz);
    }

    private <T> T mapToObject(Map<String, Object> row, Class<T> clazz) {
        try {
            T obj = clazz.getDeclaredConstructor().newInstance();
            for (Field field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                if (row.containsKey(field.getName())) {
                    field.set(obj, row.get(field.getName()));
                }
            }
            return obj;
        } catch (Exception e) {
            throw new RuntimeException("DTO 매핑 실패", e);
        }
    }

    public List<Long> selectLongs() {
        List<Map<String, Object>> rows = selectRows();
        List<Long> list = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object val = row.values().iterator().next();
            list.add(val == null ? null : ((Number) val).longValue());
        }
        return list;
    }

    private Object firstValue() {
        Map<String, Object> row = selectRow();
        return row == null ? null : row.values().iterator().next();
    }

    public Long selectLong() {
        Object val = firstValue();
        return val == null ? null : ((Number) val).longValue();
    }

    public String selectString() {
        Object val = firstValue();
        return val == null ? null : val.toString();
    }

    public Boolean selectBoolean() {
        Object val = firstValue();
        if (val == null) return null;
        if (val instanceof Number n) return n.intValue() != 0;
        return Boolean.parseBoolean(val.toString());
    }

    public LocalDateTime selectDatetime() {
        Object val = firstValue();
        return val == null ? null : (LocalDateTime) val;
    }

    private void clear() {
        sqlBuilder.setLength(0);
        params.clear();
    }
}
