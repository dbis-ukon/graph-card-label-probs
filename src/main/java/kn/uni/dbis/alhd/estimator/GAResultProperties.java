package kn.uni.dbis.alhd.estimator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Stores important logical properties of a set of matched subgraphs.
 *
 * Be careful: The node label and relationship type maps are supposed to be
 * immutable. Use the helper methods of this class to access them, otherwise
 * you may get in conflict with this assumption.
 */
public class GAResultProperties {

    /** Map from variables to associated node label fractions. */
    private final Map<String, Map<Integer, Double>> nodeLabelMap;

    /** Map from variables to associated relationship types. */
    private final Map<String, Set<Integer>> relationshipTypeMap;

    /** Whether this is the set of subgraphs containing all single nodes from the db. */
    private final boolean isInitial;

    /** Number of matched subgraphs. */
    private final double matchedSubgraphs;

    /**
     * Initialize with all information. Throws an exception if the node and
     * relationship variables are not disjunct.
     *
     * @param nodeLabelMap map from node variables to node label fractions
     * @param relationshipTypeMap map from relationship variables to an (optional) associated type
     * @param size the number of matched subgraphs
     * @param isInitial whether this is the set of all single node subgraphs
     */
    public GAResultProperties(final Map<String, Map<Integer, Double>> nodeLabelMap,
                              final Map<String, Set<Integer>> relationshipTypeMap,
                              final double size,
                              final boolean isInitial) {
        final Set<String> sharedVariables = new HashSet<>(nodeLabelMap.keySet());
        sharedVariables.retainAll(relationshipTypeMap.keySet());

        if (!sharedVariables.isEmpty()) {
            throw new IllegalArgumentException("Node and relationship variables must be disjoint.");
        }

        this.nodeLabelMap = nodeLabelMap;
        this.relationshipTypeMap = relationshipTypeMap;
        this.matchedSubgraphs = size;
        this.isInitial = isInitial;
    }

    /**
     * Returns the node label map (immutable).
     *
     * @return the node label map
     */
    public Map<String, Map<Integer, Double>> getNodeLabelMap() {
        return this.nodeLabelMap;
    }

    /**
     * Returns the relationship type map (immutable).
     *
     * @return the relationship type map
     */
    public Map<String, Set<Integer>> getRelationshipTypeMap() {
        return this.relationshipTypeMap;
    }

    /**
     * Returns the set of all node variables that are currently
     * used for the subgraph matching (immutable).
     *
     * @return all node variables in the matching
     */
    public Set<String> getNodeVariables() {
        return this.nodeLabelMap.keySet();
    }

    /**
     * Returns the set of all relationship variables that are currently
     * used for the subgraph matching (immutable).
     *
     * @return all relationship variables in the matching
     */
    public Set<String> getRelationshipVariables() {
        return this.relationshipTypeMap.keySet();
    }

    /**
     * Creates and returns the set of all node and relationship variables that are currently
     * used for the subgraph matching (mutable).
     *
     * @return all variables in the matching
     */
    public Set<String> getAllVariables() {
        final Set<String> all = new HashSet<>();
        all.addAll(this.getNodeVariables());
        all.addAll(this.getRelationshipVariables());
        return all;
    }

    /**
     * Returns the given node labels sorted descendingly by the fraction of nodes
     * matched by the given variable which have the respective label.
     * In case of equal fractions, the label with the lower number of nodes in the
     * database comes first.
     *
     * @param variable the node variable to use for sorting by the label fractions
     * @param labels the labels to sort
     * @param dbProps the database properties used for sorting by recall
     * @return the given labels sorted descendingly by fraction and recall
     */
    public List<Integer> sortLabelsByFractionAndRecall(final String variable, final Set<Integer> labels, final GADbProperties dbProps) {
        final Map<Integer, Double> fractions = this.nodeLabelMap.get(variable);
        return labels.stream().sorted((Integer l1, Integer l2) -> {
            final int betterFit = Double.compare(fractions.get(l2) / dbProps.nodes(l2), fractions.get(l1) / dbProps.nodes(l1));
            if (betterFit != 0) {
                return betterFit;
            } else {
                // in case of equal fit, we choose the label with the higher fraction
                return Double.compare(fractions.get(l2), fractions.get(l1));
            }
        }).collect(Collectors.toList());
    }

    public List<Integer> sortLabelsByFractionAndRecall(final String var1, final String var2, final Set<Integer> labels, final GADbProperties dbProps) {
        final Map<Integer, Double> fracs1 = this.nodeLabelMap.get(var1);
        final Map<Integer, Double> fracs2 = this.nodeLabelMap.get(var2);
        return labels.stream().sorted((Integer l1, Integer l2) -> {
            final int betterFit = Double.compare(Math.max(fracs1.get(l2), fracs2.get(l2)) / dbProps.nodes(l2),
            		Math.max(fracs1.get(l1), fracs2.get(l1)) / dbProps.nodes(l1));
            if (betterFit != 0) {
                return betterFit;
            } else {
                // in case of equal fit, we choose the label with the higher fraction
                return Double.compare(Math.max(fracs1.get(l2), fracs2.get(l2)), Math.max(fracs1.get(l1), fracs2.get(l1)));
            }
        }).collect(Collectors.toList());
    }

    /**
     * Returns the allowed relationship types set for the given relationship variable
     * or null if the variable is not currently matched (immutable).
     *
     * @param variable the relationship variable in question
     * @return the allowed types of the relationship variable or null
     */
    public Set<Integer> getTypes(final String variable) {
        return this.relationshipTypeMap.get(variable);
    }

    public Map<Integer, Double> getLabels(final String variable) {
        return Objects.requireNonNull(this.nodeLabelMap.get(variable));
    }

    /**
     * Returns whether a given variable matches any node in the result.
     *
     * @param variable the variable in question
     * @return whether this variable matches nodes in the result
     */
    public boolean anyNodeMatchedBy(final String variable) {
        return nodeLabelMap.containsKey(variable);
    }

    /**
     * Returns whether a given variable matches any relationship in the result.
     *
     * @param variable the variable in question
     * @return whether this variable matches relationships in the result
     */
    public boolean anyRelationshipMatchedBy(final String variable) {
        return relationshipTypeMap.containsKey(variable);
    }

    /**
     * Returns the number of matched subgraphs.
     *
     * @return how many subgraphs are matched
     */
    public double getSize() {
        return matchedSubgraphs;
    }

    /**
     * Whether this set of subgraphs simply contains all single nodes.
     *
     * @return whether this is the set of all single node subgraphs
     */
    public boolean isInitial() {
        return isInitial;
    }

    @Override
    public String toString() {
        return "[size: " + this.matchedSubgraphs + "]";
    }
}
