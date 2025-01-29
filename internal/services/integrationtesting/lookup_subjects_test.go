//go:build ci && docker && !skipintegrationtests
// +build ci,docker,!skipintegrationtests

package integrationtesting_test

import (
	"context"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
	"time"
)

func TestLookupSubjects(t *testing.T) {
	tests := map[string]struct {
		limit          int
		assertSubjects func(t *testing.T, subjectIds []string)
	}{
		"without limit": {
			assertSubjects: func(t *testing.T, subjectIds []string) {
				assert.Len(t, subjectIds, 2)
				assert.Contains(t, subjectIds, "tom")
				assert.Contains(t, subjectIds, "jill")
			},
		},
		"with limit": {
			limit: 1,
			assertSubjects: func(t *testing.T, subjectIds []string) {
				assert.Len(t, subjectIds, 1)
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, client := setupTest(t)
			response, err := client.LookupSubjects(ctx, &v1.LookupSubjectsRequest{
				Resource:          &v1.ObjectReference{ObjectType: "resource", ObjectId: "foo"},
				Permission:        "view",
				SubjectObjectType: "user",
			})
			require.NoError(t, err)
			require.NotNil(t, response)
			subjects := make([]string, 0)
			for {
				recv, recvErr := response.Recv()
				if recvErr != nil {
					if recvErr == io.EOF {
						break
					} else {
						require.NoError(t, recvErr)
					}
				}
				subjects = append(subjects, recv.GetSubject().GetSubjectObjectId())
			}
			test.assertSubjects(t, subjects)
		})
	}
}

func setupTest(t *testing.T) (context.Context, v1.PermissionsServiceClient) {
	b := testdatastore.RunDatastoreEngine(t, postgres.Engine)
	ds := b.NewDatastore(t, config.DatastoreConfigInitFunc(t,
		dsconfig.WithWatchBufferLength(0),
		dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
		dsconfig.WithRevisionQuantization(10),
		dsconfig.WithMaxRetries(50),
		dsconfig.WithRequestHedgingEnabled(false)))

	connections, cleanup := testserver.TestClusterWithDispatch(t, 1, ds)
	t.Cleanup(cleanup)

	schemaClient := v1.NewSchemaServiceClient(connections[0])
	_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `
			definition user {}
			definition resource {
				relation parent: resource
				relation viewer: user
				permission view = viewer + parent->view
			}`,
	})
	require.NoError(t, err)

	client := v1.NewPermissionsServiceClient(connections[0])

	ctx := context.Background()
	_, err = client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:foo#viewer@user:tom")),
			},
			{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:foo#parent@resource:bar")),
			},
			{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:bar#viewer@user:jill")),
			},
		},
	})
	require.NoError(t, err)

	return ctx, client
}
