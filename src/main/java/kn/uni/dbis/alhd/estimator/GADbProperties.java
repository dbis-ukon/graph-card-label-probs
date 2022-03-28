package kn.uni.dbis.alhd.estimator;

import kn.uni.dbis.alhd.queries.Direction;

import java.util.HashSet;
import java.util.Set;

/**
 * Represents the global logical properties of the database.
 */
public abstract class GADbProperties {
    /** This set describes the relations between labels. It partitions the set of labels into maximal sets of labels
     * which overlap. This is mandatory and has to include all node labels which exist in the db. */
    private final LabelDistribution labelDistribution;

    /** The set of labels in the database. */
    private final Set<Integer> labels;

    /**
     * Generates new global logical properties using the given assumption.
     *
     * @param labelDistribution how node labels are distributed in the database
     */
    public GADbProperties(final LabelDistribution labelDistribution) {
        this.labelDistribution = labelDistribution;

        this.labels = new HashSet<>();
        this.labelDistribution.getLabelHierarchy().forEach(this.labels::addAll);
    }

    /**
     * Returns the set of node labels used in the database.
     *
     * @return the set of labels currently used
     */
    public Set<Integer> labels() {
        return this.labels;
    }

    /**
     * Number of nodes with a particular label.
     *
     * @param label the wanted label
     * @return how many nodes have that label
     */
    public abstract double nodes(int label);

    /**
     * Returns the assumed distribution of node labels in this database.
     *
     * @return the distribution of node labels in the db
     */
    public LabelDistribution getLabelDistribution() {
        return this.labelDistribution;
    }

    /**
     * Number of relationships of a certain type, starting and ending at nodes with
     * particular labels, going into a particular direction.
     *
     * @param labelAtBase the wanted label at the base nodes
     * @param type the allowed type of the relationships
     * @param labelAtTarget the wanted label at the target nodes
     * @param direction the direction of the edge
     * @return how many relationships fulfil the specified label and type constraints
     */
    public abstract double relationships(int labelAtBase, int type, int labelAtTarget, Direction direction);

    /**
     * Number of relationships having one the given types, starting and ending at nodes with
     * particular labels, going into a particular direction.
     *
     * @param labelAtBase the wanted label at the base nodes
     * @param types the allowed type of the relationships
     * @param labelAtTarget the wanted label at the target nodes
     * @param direction the direction of the edge
     * @return how many relationships fulfil the specified label and type constraints
     */
    public double relationships(final int labelAtBase, final Set<Integer> types,
            final int labelAtTarget, final Direction direction) {
        return types.stream().mapToDouble(t -> this.relationships(labelAtBase, t, labelAtTarget, direction)).sum();
    }

    /**
     * Average number of relationships per node, where the relationship has one the given types and
     * starts and ends at nodes with particular labels, going into a particular direction.
     *
     * @param labelAtBase the wanted label at the base nodes
     * @param types the allowed type of the relationships
     * @param labelAtTarget the wanted label at the target nodes
     * @param direction the direction of the edge
     * @return how many relationships per node fulfil the specified label and type constraints
     */
    public double averageDegree(final int labelAtBase, final Set<Integer> types,
                                final int labelAtTarget, final Direction direction) {
        return this.relationships(labelAtBase, types, labelAtTarget, direction) / this.nodes(labelAtBase);
    }

    /**
     * Selectivity of a property at nodes with a particular label.
     *
     * @param label the wanted label
     * @param property the property in question
     * @return probability that the property equals a particular value
     *   at nodes with the given label
     */
    public abstract double sel(int label, int property);

    /**
     * Selectivity of a property at nodes with a particular label and a particular value.
     *
     * @param label the wanted label
     * @param property the property in question
     * @return probability that the property equals a particular value
     *   at nodes with the given label
     */
    public abstract double sel(int label, int property, int valueHash);

    public abstract double rangeSel(int label, int property, double min, double max);

    /**
     * Selectivity of a property at relationships with a particular type.
     *
     * @param type the wanted type
     * @param property the property in question
     * @return probability that the property equals a particular value
     *   at nodes with the given label
     */
    public abstract double relSel(int type, int property);

    /**
     * Selectivity of a property at relationships with a particular type and a particular value.
     *
     * @param type the wanted type
     * @param property the property in question
     * @return probability that the property equals a particular value
     *   at nodes with the given label
     */
    public abstract double relSel(int type, int property, int valueHash);

    public abstract double rangeRelSel(int label, int property, double min, double max);
}
