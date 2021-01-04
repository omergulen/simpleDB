package simpledb.query;

import simpledb.plan.Plan;
import simpledb.record.*;

/**
 * A term is a comparison between two expressions.
 * 
 * @author Edward Sciore
 *
 */
public class Term {
	private Expression lhs, rhs;
	private int operator = -1;
	public static final int EQ = 0, LT = 1, GT = 2, ISNULL = 3;

	/**
	 * Create a new term that compares two expressions for equality.
	 * 
	 * @param lhs the LHS expression
	 * @param rhs the RHS expression
	 */
	public Term(Expression lhs, Expression rhs) {
		this(lhs, rhs, EQ);
	}

	public Term(Expression lhs, int operator) {
		this(lhs, null, operator);
	}

	public Term(Expression lhs, Expression rhs, int operator) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.operator = operator;
	}

	/**
	 * Return true if both of the term's expressions evaluate to the same constant,
	 * with respect to the specified scan.
	 * 
	 * @param s the scan
	 * @return true if both expressions have the same value in the scan
	 */
	public boolean isSatisfied(Scan s) {
		Constant lhsval = lhs.evaluate(s);
		if (operator == ISNULL) {
			return lhsval.isNull();
		}

		Constant rhsval = rhs.evaluate(s);

		switch (operator) {
		case EQ:
			return rhsval.equals(lhsval);
		case LT:
			return lhsval.compareTo(rhsval) == -1;
		case GT:
			return lhsval.compareTo(rhsval) == 1;
		default:
			return false;
		}
	}

	/**
	 * Calculate the extent to which selecting on the term reduces the number of
	 * records output by a query. For example if the reduction factor is 2, then the
	 * term cuts the size of the output in half.
	 * 
	 * @param p the query's plan
	 * @return the integer reduction factor.
	 */
	public int reductionFactor(Plan p) {
		String lhsName, rhsName;
		if (lhs.isFieldName() && rhs.isFieldName()) {
			lhsName = lhs.asFieldName();
			rhsName = rhs.asFieldName();
			return Math.max(p.distinctValues(lhsName), p.distinctValues(rhsName));
		}
		if (lhs.isFieldName()) {
			lhsName = lhs.asFieldName();
			return p.distinctValues(lhsName);
		}
		if (rhs.isFieldName()) {
			rhsName = rhs.asFieldName();
			return p.distinctValues(rhsName);
		}

		switch (operator) {
		case EQ:
			return 1;
		case LT:
		case GT:
			return 2;
		case ISNULL:
			return 10;
		default:
			return Integer.MAX_VALUE;
		}

	}

	/**
	 * Determine if this term is of the form "F=c" where F is the specified field
	 * and c is some constant. If so, the method returns that constant. If not, the
	 * method returns null.
	 * 
	 * @param fldname the name of the field
	 * @return either the constant or null
	 */
	public Constant equatesWithConstant(String fldname) {
		if (operator != EQ)
			return null;

		if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && !rhs.isFieldName())
			return rhs.asConstant();
		else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && !lhs.isFieldName())
			return lhs.asConstant();
		else
			return null;
	}

	/**
	 * Determine if this term is of the form "F1=F2" where F1 is the specified field
	 * and F2 is another field. If so, the method returns the name of that field. If
	 * not, the method returns null.
	 * 
	 * @param fldname the name of the field
	 * @return either the name of the other field, or null
	 */
	public String equatesWithField(String fldname) {
		if (operator != EQ)
			return null;

		if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && rhs.isFieldName())
			return rhs.asFieldName();
		else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && lhs.isFieldName())
			return lhs.asFieldName();
		else
			return null;
	}

	/**
	 * Return true if both of the term's expressions apply to the specified schema.
	 * 
	 * @param sch the schema
	 * @return true if both expressions apply to the schema
	 */
	public boolean appliesTo(Schema sch) {
		return lhs.appliesTo(sch) && (operator == ISNULL || rhs.appliesTo(sch));
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(lhs.toString());
		switch (operator) {
		case EQ:
			sb.append(" = ");
			break;
		case LT:
			sb.append(" < ");
			break;
		case GT:
			sb.append(" > ");
			break;
		case ISNULL:
			sb.append(" is null");
			return sb.toString();
		default:
			return sb.toString();
		}

		sb.append(rhs.toString());
		return sb.toString();
	}
}
