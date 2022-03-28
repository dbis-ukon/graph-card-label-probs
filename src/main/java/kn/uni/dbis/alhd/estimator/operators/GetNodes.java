package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GADbProperties;
import kn.uni.dbis.alhd.estimator.GAResultProperties;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the GetNodes-operator.
 */
public class GetNodes implements GALogicalOperator {
    /** Database properties. */
    private final GADbProperties dbProps;

    /** Variable that matches the nodes in the result of this operator. */
    private final String variable;

    /**
     * Constructs a new GetNodes-operator.
     *
     * The GetNodes-operator creates a new set of subgraphs, each of which
     * is a single node. The nodes are matched with the specified variable.
     *
     * @param dbProps database properties
     * @param variable the variable that matches the nodes
     */
    public GetNodes(final GADbProperties dbProps, final String variable) {
        this.dbProps = dbProps;
        this.variable = variable;
    }

    @Override
    public GAResultProperties computeLogicalProperties(final List<GAResultProperties> inputProperties) {
        final Map<String, Map<Integer, Double>> nodeLabelMap = new HashMap<>(1);
        final Map<Integer, Double> probabilities = new HashMap<>(this.dbProps.labels().size());
        for (int l : this.dbProps.labels()) {
            probabilities.put(l, this.dbProps.nodes(l) / this.dbProps.nodes(-1));
        }
        nodeLabelMap.put(this.variable, probabilities);
        return new GAResultProperties(nodeLabelMap,
                Collections.emptyMap(), // no relationships are matched yet
                this.dbProps.nodes(-1), // number of matched subgraphs == number of all nodes
                true); // empty label set yields this result
    }

    @Override
    public int hash() {
        return this.variable.hashCode();
    }

    @Override
    public boolean eq(final GALogicalOperator other) {
        return this == other || other instanceof GetNodes && ((GetNodes) other).variable.equals(this.variable);
    }

    /**
     * Returns this operator's database properties.
     *
     * @return database properties
     */
    public GADbProperties getDBProperties() {
        return this.dbProps;
    }

    /**
     * Returns the variable that matches the nodes in the result of this operator.
     *
     * @return variable that matches the nodes.
     */
    public String getVariable() {
        return variable;
    }

    @Override
    public String toString() {
        return "GetNodes_" + this.getVariable();
    }
}
