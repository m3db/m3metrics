package changes

import (
	"encoding/json"
	"testing"

	"github.com/m3db/m3metrics/rules/models"
	"github.com/stretchr/testify/require"
)

func TestRuleSetChangeSetsSort(t *testing.T) {
	expected := RuleSetChangeSets{
		RuleSetChangeSet{
			Namespace: "service1",
			MappingRuleChanges: []MappingRuleChange{
				{
					Op: AddOp,
					RuleData: &models.MappingRule{
						Name: "Add1",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.MappingRule{
						Name: "Add2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "mrID2",
					RuleData: &models.MappingRule{
						Name: "change1",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "mrID3",
					RuleData: &models.MappingRule{
						Name: "change2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "mrID1",
					RuleData: &models.MappingRule{
						Name: "change3",
					},
				},
			},
			RollupRuleChanges: []RollupRuleChange{
				{
					Op: AddOp,
					RuleData: &models.RollupRule{
						Name: "Add1",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.RollupRule{
						Name: "Add2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "rrID2",
					RuleData: &models.RollupRule{
						Name: "change1",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "rrID3",
					RuleData: &models.RollupRule{
						Name: "change2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "rrID1",
					RuleData: &models.RollupRule{
						Name: "change3",
					},
				},
			},
		},
		RuleSetChangeSet{
			Namespace: "service2",
		},
		RuleSetChangeSet{
			Namespace: "service3",
		},
	}

	ruleSets.Sort()
	require.Equal(t, expected[0].RollupRuleChanges, ruleSets[0].RollupRuleChanges)
	require.Equal(t, expected[0].MappingRuleChanges, ruleSets[0].MappingRuleChanges)
}

func TestUpdateOpMarshalJSON(t *testing.T) {
	testData := []UpdateOp{
		UnknownOp, AddOp, RemoveOp, ChangeOp,
	}
	expected := []string{
		"unknown", "add", "remove", "change",
	}

	for i, value := range expected {
		expectedBytes, err := json.Marshal(value)
		require.NoError(t, err)
		actualBytes, err := testData[i].MarshalJSON()
		require.NoError(t, err)
		require.Equal(t, expectedBytes, actualBytes)
	}
}

func TestUpdateOpUnMarshalJSON(t *testing.T) {
	expectedOps := []UpdateOp{
		UnknownOp, AddOp, RemoveOp, ChangeOp,
	}
	testStrings := []string{
		"unknown", "add", "remove", "change",
	}
	testBytes := make([][]byte, len(testStrings))
	for i, value := range testStrings {
		b, err := json.Marshal(value)
		require.NoError(t, err)
		testBytes[i] = b
	}

	for i, value := range expectedOps {
		var op UpdateOp
		err := op.UnmarshalJSON(testBytes[i])
		if value == UnknownOp {
			require.Equal(t, "unknown update op unknown", err.Error())
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, op, value)
	}
}

func TestUpdateOpUnMarshalJSONError(t *testing.T) {
	var testBytes []byte
	b, err := json.Marshal(1)
	require.NoError(t, err)
	testBytes = b

	var op UpdateOp
	err = op.UnmarshalJSON(testBytes)
	require.Error(t, err)
}

var (
	ruleSets = RuleSetChangeSets{
		RuleSetChangeSet{
			Namespace: "service3",
		},
		RuleSetChangeSet{
			Namespace: "service2",
		},
		RuleSetChangeSet{
			Namespace: "service1",
			MappingRuleChanges: []MappingRuleChange{
				{
					Op:     ChangeOp,
					RuleID: "mrID1",
					RuleData: &models.MappingRule{
						Name: "change3",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.MappingRule{
						Name: "Add2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "mrID2",
					RuleData: &models.MappingRule{
						Name: "change1",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "mrID3",
					RuleData: &models.MappingRule{
						Name: "change2",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.MappingRule{
						Name: "Add1",
					},
				},
			},
			RollupRuleChanges: []RollupRuleChange{
				{
					Op:     ChangeOp,
					RuleID: "rrID1",
					RuleData: &models.RollupRule{
						Name: "change3",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.RollupRule{
						Name: "Add2",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "rrID2",
					RuleData: &models.RollupRule{
						Name: "change1",
					},
				},
				{
					Op:     ChangeOp,
					RuleID: "rrID3",
					RuleData: &models.RollupRule{
						Name: "change2",
					},
				},
				{
					Op: AddOp,
					RuleData: &models.RollupRule{
						Name: "Add1",
					},
				},
			},
		},
	}
)
