package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GADbProperties;

import java.util.Set;

/**
 * Abstract superclass for all selection implementations.
 * A selection keeps all subgraphs that fulfil its selection predicate.
 */
public abstract class Selection implements GALogicalOperator {
    /** Database properties. */
    private final GADbProperties dbProps;

    /**
     * Creates a selection for a database with the given properties.
     *
     * @param dbProps database properties
     */
    Selection(final GADbProperties dbProps) {
        this.dbProps = dbProps;
    }

    /**
     * Returns all variables needed to evaluate the predicate of
     * this selection.
     *
     * @return all variables referenced in the selection
     */
    public abstract Set<String> getIncludedVariables();

    /**
     * Returns this operator's database properties.
     *
     * @return database properties
     */
    public GADbProperties getDBProperties() {
        return this.dbProps;
    }
}
