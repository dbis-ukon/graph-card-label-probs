package kn.uni.dbis.alhd.estimator;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Represents a distribution of labels in the database. This is intended to be used for
 * precise result size estimations.
 *
 * Created by Moritz Renftle on 11.12.16.
 */
public abstract class LabelDistribution {
    /** This set describes the relations between labels. It partitions the set of labels into maximal sets of labels
     * which overlap. This is mandatory and has to include all node labels which exist in the db. */
    private final Set<Set<Integer>> labelHierarchy;

    /** Map specifying known sublabel relations. May be empty if no such relations are known. */
    private final Map<Integer, Set<Integer>> sublabelMap;

    /**
     * Creates a new label distribution.
     */
    protected LabelDistribution(final Set<Set<Integer>> labelHierarchy, final Map<Integer, Set<Integer>> sublabelMap) {
        this.labelHierarchy = labelHierarchy;
        this.sublabelMap = sublabelMap;
    }

    /**
     * Returns the label hierarchy in the database.
     *
     * @return the label hierarchy
     */
    public Set<Set<Integer>> getLabelHierarchy() {
        return labelHierarchy;
    }

    /**
     * Returns the set of labels which are assumed to be sublabels of the given label.
     *
     * @param label the label in question
     * @return the labels which are assumed to be sublabels of the given label
     */
    public Set<Integer> getAllKnownSublabels(final Integer label) {
        return this.sublabelMap.getOrDefault(label, Collections.emptySet());
    }
}
