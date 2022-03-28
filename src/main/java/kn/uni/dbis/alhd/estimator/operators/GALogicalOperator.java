package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GAResultProperties;

import java.util.List;

public interface GALogicalOperator {
	GAResultProperties computeLogicalProperties(List<GAResultProperties> inputProperties);
	int hash();
	boolean eq(GALogicalOperator other);
}
