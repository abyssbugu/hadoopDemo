package com.abyss.pojo;

import java.util.ArrayList;
import java.util.List;

public class TFlowNumExample {
    protected String orderByClause;

    protected boolean distinct;

    protected List<Criteria> oredCriteria;

    public TFlowNumExample() {
        oredCriteria = new ArrayList<Criteria>();
    }

    public void setOrderByClause(String orderByClause) {
        this.orderByClause = orderByClause;
    }

    public String getOrderByClause() {
        return orderByClause;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public List<Criteria> getOredCriteria() {
        return oredCriteria;
    }

    public void or(Criteria criteria) {
        oredCriteria.add(criteria);
    }

    public Criteria or() {
        Criteria criteria = createCriteriaInternal();
        oredCriteria.add(criteria);
        return criteria;
    }

    public Criteria createCriteria() {
        Criteria criteria = createCriteriaInternal();
        if (oredCriteria.size() == 0) {
            oredCriteria.add(criteria);
        }
        return criteria;
    }

    protected Criteria createCriteriaInternal() {
        Criteria criteria = new Criteria();
        return criteria;
    }

    public void clear() {
        oredCriteria.clear();
        orderByClause = null;
        distinct = false;
    }

    protected abstract static class GeneratedCriteria {
        protected List<Criterion> criteria;

        protected GeneratedCriteria() {
            super();
            criteria = new ArrayList<Criterion>();
        }

        public boolean isValid() {
            return criteria.size() > 0;
        }

        public List<Criterion> getAllCriteria() {
            return criteria;
        }

        public List<Criterion> getCriteria() {
            return criteria;
        }

        protected void addCriterion(String condition) {
            if (condition == null) {
                throw new RuntimeException("Value for condition cannot be null");
            }
            criteria.add(new Criterion(condition));
        }

        protected void addCriterion(String condition, Object value, String property) {
            if (value == null) {
                throw new RuntimeException("Value for " + property + " cannot be null");
            }
            criteria.add(new Criterion(condition, value));
        }

        protected void addCriterion(String condition, Object value1, Object value2, String property) {
            if (value1 == null || value2 == null) {
                throw new RuntimeException("Between values for " + property + " cannot be null");
            }
            criteria.add(new Criterion(condition, value1, value2));
        }

        public Criteria andIdIsNull() {
            addCriterion("id is null");
            return (Criteria) this;
        }

        public Criteria andIdIsNotNull() {
            addCriterion("id is not null");
            return (Criteria) this;
        }

        public Criteria andIdEqualTo(Integer value) {
            addCriterion("id =", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotEqualTo(Integer value) {
            addCriterion("id <>", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdGreaterThan(Integer value) {
            addCriterion("id >", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdGreaterThanOrEqualTo(Integer value) {
            addCriterion("id >=", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdLessThan(Integer value) {
            addCriterion("id <", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdLessThanOrEqualTo(Integer value) {
            addCriterion("id <=", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdIn(List<Integer> values) {
            addCriterion("id in", values, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotIn(List<Integer> values) {
            addCriterion("id not in", values, "id");
            return (Criteria) this;
        }

        public Criteria andIdBetween(Integer value1, Integer value2) {
            addCriterion("id between", value1, value2, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotBetween(Integer value1, Integer value2) {
            addCriterion("id not between", value1, value2, "id");
            return (Criteria) this;
        }

        public Criteria andDatestrIsNull() {
            addCriterion("dateStr is null");
            return (Criteria) this;
        }

        public Criteria andDatestrIsNotNull() {
            addCriterion("dateStr is not null");
            return (Criteria) this;
        }

        public Criteria andDatestrEqualTo(String value) {
            addCriterion("dateStr =", value, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrNotEqualTo(String value) {
            addCriterion("dateStr <>", value, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrGreaterThan(String value) {
            addCriterion("dateStr >", value, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrGreaterThanOrEqualTo(String value) {
            addCriterion("dateStr >=", value, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrLessThan(String value) {
            addCriterion("dateStr <", value, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrLessThanOrEqualTo(String value) {
            addCriterion("dateStr <=", value, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrLike(String value) {
            addCriterion("dateStr like", value, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrNotLike(String value) {
            addCriterion("dateStr not like", value, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrIn(List<String> values) {
            addCriterion("dateStr in", values, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrNotIn(List<String> values) {
            addCriterion("dateStr not in", values, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrBetween(String value1, String value2) {
            addCriterion("dateStr between", value1, value2, "datestr");
            return (Criteria) this;
        }

        public Criteria andDatestrNotBetween(String value1, String value2) {
            addCriterion("dateStr not between", value1, value2, "datestr");
            return (Criteria) this;
        }

        public Criteria andPvnumIsNull() {
            addCriterion("pVNum is null");
            return (Criteria) this;
        }

        public Criteria andPvnumIsNotNull() {
            addCriterion("pVNum is not null");
            return (Criteria) this;
        }

        public Criteria andPvnumEqualTo(Integer value) {
            addCriterion("pVNum =", value, "pvnum");
            return (Criteria) this;
        }

        public Criteria andPvnumNotEqualTo(Integer value) {
            addCriterion("pVNum <>", value, "pvnum");
            return (Criteria) this;
        }

        public Criteria andPvnumGreaterThan(Integer value) {
            addCriterion("pVNum >", value, "pvnum");
            return (Criteria) this;
        }

        public Criteria andPvnumGreaterThanOrEqualTo(Integer value) {
            addCriterion("pVNum >=", value, "pvnum");
            return (Criteria) this;
        }

        public Criteria andPvnumLessThan(Integer value) {
            addCriterion("pVNum <", value, "pvnum");
            return (Criteria) this;
        }

        public Criteria andPvnumLessThanOrEqualTo(Integer value) {
            addCriterion("pVNum <=", value, "pvnum");
            return (Criteria) this;
        }

        public Criteria andPvnumIn(List<Integer> values) {
            addCriterion("pVNum in", values, "pvnum");
            return (Criteria) this;
        }

        public Criteria andPvnumNotIn(List<Integer> values) {
            addCriterion("pVNum not in", values, "pvnum");
            return (Criteria) this;
        }

        public Criteria andPvnumBetween(Integer value1, Integer value2) {
            addCriterion("pVNum between", value1, value2, "pvnum");
            return (Criteria) this;
        }

        public Criteria andPvnumNotBetween(Integer value1, Integer value2) {
            addCriterion("pVNum not between", value1, value2, "pvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumIsNull() {
            addCriterion("uVNum is null");
            return (Criteria) this;
        }

        public Criteria andUvnumIsNotNull() {
            addCriterion("uVNum is not null");
            return (Criteria) this;
        }

        public Criteria andUvnumEqualTo(Integer value) {
            addCriterion("uVNum =", value, "uvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumNotEqualTo(Integer value) {
            addCriterion("uVNum <>", value, "uvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumGreaterThan(Integer value) {
            addCriterion("uVNum >", value, "uvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumGreaterThanOrEqualTo(Integer value) {
            addCriterion("uVNum >=", value, "uvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumLessThan(Integer value) {
            addCriterion("uVNum <", value, "uvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumLessThanOrEqualTo(Integer value) {
            addCriterion("uVNum <=", value, "uvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumIn(List<Integer> values) {
            addCriterion("uVNum in", values, "uvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumNotIn(List<Integer> values) {
            addCriterion("uVNum not in", values, "uvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumBetween(Integer value1, Integer value2) {
            addCriterion("uVNum between", value1, value2, "uvnum");
            return (Criteria) this;
        }

        public Criteria andUvnumNotBetween(Integer value1, Integer value2) {
            addCriterion("uVNum not between", value1, value2, "uvnum");
            return (Criteria) this;
        }

        public Criteria andIpnumIsNull() {
            addCriterion("iPNum is null");
            return (Criteria) this;
        }

        public Criteria andIpnumIsNotNull() {
            addCriterion("iPNum is not null");
            return (Criteria) this;
        }

        public Criteria andIpnumEqualTo(Integer value) {
            addCriterion("iPNum =", value, "ipnum");
            return (Criteria) this;
        }

        public Criteria andIpnumNotEqualTo(Integer value) {
            addCriterion("iPNum <>", value, "ipnum");
            return (Criteria) this;
        }

        public Criteria andIpnumGreaterThan(Integer value) {
            addCriterion("iPNum >", value, "ipnum");
            return (Criteria) this;
        }

        public Criteria andIpnumGreaterThanOrEqualTo(Integer value) {
            addCriterion("iPNum >=", value, "ipnum");
            return (Criteria) this;
        }

        public Criteria andIpnumLessThan(Integer value) {
            addCriterion("iPNum <", value, "ipnum");
            return (Criteria) this;
        }

        public Criteria andIpnumLessThanOrEqualTo(Integer value) {
            addCriterion("iPNum <=", value, "ipnum");
            return (Criteria) this;
        }

        public Criteria andIpnumIn(List<Integer> values) {
            addCriterion("iPNum in", values, "ipnum");
            return (Criteria) this;
        }

        public Criteria andIpnumNotIn(List<Integer> values) {
            addCriterion("iPNum not in", values, "ipnum");
            return (Criteria) this;
        }

        public Criteria andIpnumBetween(Integer value1, Integer value2) {
            addCriterion("iPNum between", value1, value2, "ipnum");
            return (Criteria) this;
        }

        public Criteria andIpnumNotBetween(Integer value1, Integer value2) {
            addCriterion("iPNum not between", value1, value2, "ipnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumIsNull() {
            addCriterion("newUvNum is null");
            return (Criteria) this;
        }

        public Criteria andNewuvnumIsNotNull() {
            addCriterion("newUvNum is not null");
            return (Criteria) this;
        }

        public Criteria andNewuvnumEqualTo(Integer value) {
            addCriterion("newUvNum =", value, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumNotEqualTo(Integer value) {
            addCriterion("newUvNum <>", value, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumGreaterThan(Integer value) {
            addCriterion("newUvNum >", value, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumGreaterThanOrEqualTo(Integer value) {
            addCriterion("newUvNum >=", value, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumLessThan(Integer value) {
            addCriterion("newUvNum <", value, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumLessThanOrEqualTo(Integer value) {
            addCriterion("newUvNum <=", value, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumIn(List<Integer> values) {
            addCriterion("newUvNum in", values, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumNotIn(List<Integer> values) {
            addCriterion("newUvNum not in", values, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumBetween(Integer value1, Integer value2) {
            addCriterion("newUvNum between", value1, value2, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andNewuvnumNotBetween(Integer value1, Integer value2) {
            addCriterion("newUvNum not between", value1, value2, "newuvnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumIsNull() {
            addCriterion("visitNum is null");
            return (Criteria) this;
        }

        public Criteria andVisitnumIsNotNull() {
            addCriterion("visitNum is not null");
            return (Criteria) this;
        }

        public Criteria andVisitnumEqualTo(Integer value) {
            addCriterion("visitNum =", value, "visitnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumNotEqualTo(Integer value) {
            addCriterion("visitNum <>", value, "visitnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumGreaterThan(Integer value) {
            addCriterion("visitNum >", value, "visitnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumGreaterThanOrEqualTo(Integer value) {
            addCriterion("visitNum >=", value, "visitnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumLessThan(Integer value) {
            addCriterion("visitNum <", value, "visitnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumLessThanOrEqualTo(Integer value) {
            addCriterion("visitNum <=", value, "visitnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumIn(List<Integer> values) {
            addCriterion("visitNum in", values, "visitnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumNotIn(List<Integer> values) {
            addCriterion("visitNum not in", values, "visitnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumBetween(Integer value1, Integer value2) {
            addCriterion("visitNum between", value1, value2, "visitnum");
            return (Criteria) this;
        }

        public Criteria andVisitnumNotBetween(Integer value1, Integer value2) {
            addCriterion("visitNum not between", value1, value2, "visitnum");
            return (Criteria) this;
        }
    }

    public static class Criteria extends GeneratedCriteria {

        protected Criteria() {
            super();
        }
    }

    public static class Criterion {
        private String condition;

        private Object value;

        private Object secondValue;

        private boolean noValue;

        private boolean singleValue;

        private boolean betweenValue;

        private boolean listValue;

        private String typeHandler;

        public String getCondition() {
            return condition;
        }

        public Object getValue() {
            return value;
        }

        public Object getSecondValue() {
            return secondValue;
        }

        public boolean isNoValue() {
            return noValue;
        }

        public boolean isSingleValue() {
            return singleValue;
        }

        public boolean isBetweenValue() {
            return betweenValue;
        }

        public boolean isListValue() {
            return listValue;
        }

        public String getTypeHandler() {
            return typeHandler;
        }

        protected Criterion(String condition) {
            super();
            this.condition = condition;
            this.typeHandler = null;
            this.noValue = true;
        }

        protected Criterion(String condition, Object value, String typeHandler) {
            super();
            this.condition = condition;
            this.value = value;
            this.typeHandler = typeHandler;
            if (value instanceof List<?>) {
                this.listValue = true;
            } else {
                this.singleValue = true;
            }
        }

        protected Criterion(String condition, Object value) {
            this(condition, value, null);
        }

        protected Criterion(String condition, Object value, Object secondValue, String typeHandler) {
            super();
            this.condition = condition;
            this.value = value;
            this.secondValue = secondValue;
            this.typeHandler = typeHandler;
            this.betweenValue = true;
        }

        protected Criterion(String condition, Object value, Object secondValue) {
            this(condition, value, secondValue, null);
        }
    }
}