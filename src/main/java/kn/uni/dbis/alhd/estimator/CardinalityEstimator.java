package kn.uni.dbis.alhd.estimator;

import kn.uni.dbis.alhd.queries.CypherPattern;
import kn.uni.dbis.alhd.queries.CypherQuery;
import kn.uni.dbis.alhd.statistics.GraphStatistics;

public final class CardinalityEstimator {

	private final GraphStatistics stats;
	private final GADbProperties dbProps;

	public CardinalityEstimator(final GraphStatistics stats, final LabelDistribution dist, final boolean simplified) {
		this.stats = stats;
		this.dbProps = new GraphDBProperties(dist, stats, simplified);
	}

	public double estimate(final CypherPattern pattern, final boolean useNewJoin, final Double staticSelectivity) {
		final Expression expression = PatternToTreeConverter.mapToAlgebraExpression(this.stats, this.dbProps, pattern, useNewJoin, staticSelectivity);
		return expression.getLogicalProperties().getSize();
	}

	public double estimate_(final CypherPattern pattern) {
		return estimate(pattern, true, null);
	}

	public double estimate(final CypherQuery query, final boolean useNewJoin, final Double staticSelectivity) {
		final Expression expression = PatternToTreeConverter.mapToAlgebraExpression(this.stats, this.dbProps, query, useNewJoin, staticSelectivity);
		return expression.getLogicalProperties().getSize();
	}

	public double estimate_(final CypherQuery query) {
		return estimate(query, true, null);
	}
}
