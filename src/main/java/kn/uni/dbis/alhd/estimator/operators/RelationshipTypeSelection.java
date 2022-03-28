package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GADbProperties;
import kn.uni.dbis.alhd.estimator.GAResultProperties;
import kn.uni.dbis.alhd.queries.Direction;

import java.util.*;

/**
 * Implementation of the RelationshipTypeSelection-Operator.
 *
 * The relationship type selection keeps all subgraphs from the input, where the relationship
 * matched by the given variable has one of the given types.
 */
public class RelationshipTypeSelection extends Selection {

    /** The selection variable. */
    private final String variable;

    /** The relationship types allowed for relationships matched by the selection variable. */
    private final Set<Integer> allowedTypes;

    /**
     * Constructs a new RelationshipTypeSelection-operator.
     *
     * @param dbProps database properties
     * @param variable the relationships to select from
     * @param allowedTypes which types of these relationships will be included in the result
     */
    public RelationshipTypeSelection(final GADbProperties dbProps, final String variable,
                                     final Set<Integer> allowedTypes) {
        super(dbProps);
        this.variable = variable;
        this.allowedTypes = allowedTypes;
    }

    /**
     * Returns the selection variable.
     *
     * @return the variable used for the selection
     */
    public String getVariable() {
        return variable;
    }

    /**
     * Returns the set of types the relationships must have to be included in the result.
     *
     * @return the types allowed by this selection
     */
    public Set<Integer> getAllowedTypes() {
        return this.allowedTypes;
    }

    @Override
    public Set<String> getIncludedVariables() {
        return Collections.singleton(this.variable);
    }

    @Override
    public GAResultProperties computeLogicalProperties(final List<GAResultProperties> inputProperties) {
        final GAResultProperties input = inputProperties.get(0);
        final Set<Integer> allowedTypes = new HashSet<>(this.allowedTypes);
        allowedTypes.retainAll(input.getTypes(this.variable));

        final Map<String, Set<Integer>> relationshipTypeMap = new HashMap<>(input.getRelationshipTypeMap());
        relationshipTypeMap.put(this.variable, allowedTypes);

        final GADbProperties dbProps = this.getDBProperties();
        final double resultSize = input.getSize() * dbProps.relationships(-1, allowedTypes, -1, Direction.OUTGOING)
                / dbProps.relationships(-1, input.getTypes(this.variable), -1, Direction.OUTGOING);

        return new GAResultProperties(input.getNodeLabelMap(), relationshipTypeMap, resultSize, false);
    }

    @Override
    public int hash() {
        return Objects.hash(this.variable, this.allowedTypes);
    }

    @Override
    public boolean eq(final GALogicalOperator other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RelationshipTypeSelection)) {
            return false;
        }
        final RelationshipTypeSelection otherTypeSelection = (RelationshipTypeSelection) other;
        return otherTypeSelection.getVariable().equals(this.variable)
                && otherTypeSelection.getAllowedTypes().equals(this.allowedTypes);
    }
}
