package kn.uni.dbis.alhd.estimator;

import kn.uni.dbis.alhd.estimator.operators.GALogicalOperator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Expression {

	private final GALogicalOperator operator;
	private final Expression[] inputs;
	private final GAResultProperties logicalProperties;

	public Expression(GALogicalOperator operator, final Expression... inputs) {
		this.operator = operator;
		this.inputs = inputs;
		final List<GAResultProperties> inputProps = Arrays.stream(this.inputs).map(Expression::getLogicalProperties).collect(Collectors.toList());
		this.logicalProperties = this.operator.computeLogicalProperties(inputProps);
	}

	public GAResultProperties getLogicalProperties() {
		return this.logicalProperties;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		this.toString(sb);
		return sb.toString();
	}

	private void toString(StringBuilder sb) {
		sb.append("Expression[").append(this.operator.toString());
		for (final Expression sub : this.inputs) {
			sub.toString(sb.append(", "));
		}
		sb.append(", card=").append(this.logicalProperties != null ? this.logicalProperties.getSize() : "?").append("]");
	}
}
