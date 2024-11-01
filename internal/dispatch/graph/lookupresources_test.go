package graph

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const veryLargeLimit = 1000000000

var RR = tuple.RR

func resolvedRes(resourceID string) *v1.ResolvedResource {
	return &v1.ResolvedResource{
		ResourceId:     resourceID,
		Permissionship: v1.ResolvedResource_HAS_PERMISSION,
	}
}

func TestSimpleLookupResources(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		start                 tuple.RelationReference
		target                tuple.ObjectAndRelation
		expectedResources     []*v1.ResolvedResource
		expectedDispatchCount uint32
		expectedDepthRequired uint32
	}{
		{
			RR("document", "view"),
			ONR("user", "unknown", "..."),
			[]*v1.ResolvedResource{},
			0,
			0,
		},
		{
			RR("document", "view"),
			ONR("user", "eng_lead", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("masterplan"),
			},
			2,
			1,
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("masterplan"),
			},
			2,
			0,
		},
		{
			RR("document", "view"),
			ONR("user", "legal", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("companyplan"),
				resolvedRes("masterplan"),
			},
			6,
			3,
		},
		{
			RR("document", "view_and_edit"),
			ONR("user", "multiroleguy", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("specialplan"),
			},
			7,
			3,
		},
		{
			RR("folder", "view"),
			ONR("user", "owner", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("strategy"),
				resolvedRes("company"),
			},
			8,
			4,
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"%s#%s->%s",
			tc.start.ObjectType,
			tc.start.Relation,
			tuple.StringONR(tc.target),
		)

		tc := tc
		t.Run(name, func(t *testing.T) {
			require := require.New(t)
			ctx, dispatcher, revision := newLocalDispatcher(t)
			defer dispatcher.Close()

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
			err := dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
				ObjectRelation: tc.start.ToCoreRR(),
				Subject:        tc.target.ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: veryLargeLimit,
			}, stream)

			require.NoError(err)

			foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount := processResults(stream)
			require.ElementsMatch(tc.expectedResources, foundResources, "Found: %v, Expected: %v", foundResources, tc.expectedResources)
			require.Equal(tc.expectedDepthRequired, maxDepthRequired, "Depth required mismatch")
			require.LessOrEqual(maxDispatchCount, tc.expectedDispatchCount, "Found dispatch count greater than expected")
			require.Equal(uint32(0), maxCachedDispatchCount)

			// We have to sleep a while to let the cache converge:
			// https://github.com/outcaste-io/ristretto/blob/01b9f37dd0fd453225e042d6f3a27cd14f252cd0/cache_test.go#L17
			time.Sleep(10 * time.Millisecond)

			// Run again with the cache available.
			stream = dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
			err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
				ObjectRelation: tc.start.ToCoreRR(),
				Subject:        tc.target.ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: veryLargeLimit,
			}, stream)
			dispatcher.Close()

			require.NoError(err)

			foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount = processResults(stream)
			require.ElementsMatch(tc.expectedResources, foundResources, "Found: %v, Expected: %v", foundResources, tc.expectedResources)
			require.Equal(tc.expectedDepthRequired, maxDepthRequired, "Depth required mismatch")
			require.LessOrEqual(maxCachedDispatchCount, tc.expectedDispatchCount, "Found dispatch count greater than expected")
			require.Equal(uint32(0), maxDispatchCount)
		})
	}
}

func TestSimpleLookupResourcesWithCursor(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		subject        string
		expectedFirst  []string
		expectedSecond []string
	}{
		{
			subject:        "owner",
			expectedFirst:  []string{"ownerplan"},
			expectedSecond: []string{"companyplan", "masterplan", "ownerplan"},
		},
		{
			subject:        "chief_financial_officer",
			expectedFirst:  []string{"healthplan"},
			expectedSecond: []string{"healthplan", "masterplan"},
		},
		{
			subject:        "auditor",
			expectedFirst:  []string{"companyplan"},
			expectedSecond: []string{"companyplan", "masterplan"},
		},
	} {
		tc := tc
		t.Run(tc.subject, func(t *testing.T) {
			require := require.New(t)
			ctx, dispatcher, revision := newLocalDispatcher(t)
			defer dispatcher.Close()

			found := mapz.NewSet[string]()

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
			err := dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
				ObjectRelation: RR("document", "view").ToCoreRR(),
				Subject:        ONR("user", tc.subject, "...").ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: 1,
			}, stream)

			require.NoError(err)

			require.Equal(1, len(stream.Results()))

			found.Insert(stream.Results()[0].ResolvedResource.ResourceId)
			require.Equal(tc.expectedFirst, found.AsSlice())

			cursor := stream.Results()[0].AfterResponseCursor
			require.NotNil(cursor)

			stream = dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
			err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
				ObjectRelation: RR("document", "view").ToCoreRR(),
				Subject:        ONR("user", tc.subject, "...").ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalCursor: cursor,
				OptionalLimit:  2,
			}, stream)

			require.NoError(err)

			for _, result := range stream.Results() {
				found.Insert(result.ResolvedResource.ResourceId)
			}

			foundResults := found.AsSlice()
			slices.Sort(foundResults)

			require.Equal(tc.expectedSecond, foundResults)
		})
	}
}

func TestLookupResourcesCursorStability(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	ctx, dispatcher, revision := newLocalDispatcher(t)
	defer dispatcher.Close()

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)

	// Make the first first request.
	err := dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view").ToCoreRR(),
		Subject:        ONR("user", "owner", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		OptionalLimit: 2,
	}, stream)

	require.NoError(err)
	require.Equal(2, len(stream.Results()))

	cursor := stream.Results()[1].AfterResponseCursor
	require.NotNil(cursor)

	// Make the same request and ensure the cursor has not changed.
	stream = dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
	err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view").ToCoreRR(),
		Subject:        ONR("user", "owner", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		OptionalLimit: 2,
	}, stream)

	require.NoError(err)

	require.NoError(err)
	require.Equal(2, len(stream.Results()))

	cursorAgain := stream.Results()[1].AfterResponseCursor
	require.NotNil(cursor)
	require.Equal(cursor, cursorAgain)
}

func processResults(stream *dispatch.CollectingDispatchStream[*v1.DispatchLookupResourcesResponse]) ([]*v1.ResolvedResource, uint32, uint32, uint32) {
	foundResources := []*v1.ResolvedResource{}
	var maxDepthRequired uint32
	var maxDispatchCount uint32
	var maxCachedDispatchCount uint32
	for _, result := range stream.Results() {
		foundResources = append(foundResources, result.ResolvedResource)
		maxDepthRequired = max(maxDepthRequired, result.Metadata.DepthRequired)
		maxDispatchCount = max(maxDispatchCount, result.Metadata.DispatchCount)
		maxCachedDispatchCount = max(maxCachedDispatchCount, result.Metadata.CachedDispatchCount)
	}
	return foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount
}

func TestMaxDepthLookup(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10, 100)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)

	err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view").ToCoreRR(),
		Subject:        ONR("user", "legal", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 0,
		},
	}, stream)

	require.Error(err)
}

func joinTuples(first []tuple.Relationship, others ...[]tuple.Relationship) []tuple.Relationship {
	current := first
	for _, second := range others {
		current = append(current, second...)
	}
	return current
}

func genRelsWithOffset(resourceName string, relation string, subjectName string, subjectID string, offset int, number int) []tuple.Relationship {
	return genRelsWithCaveat(resourceName, relation, subjectName, subjectID, "", nil, offset, number)
}

func genRels(resourceName string, relation string, subjectName string, subjectID string, number int) []tuple.Relationship {
	return genRelsWithOffset(resourceName, relation, subjectName, subjectID, 0, number)
}

func genSubjectRels(resourceName string, relation string, subjectName string, subjectRelation string, number int) []tuple.Relationship {
	rels := make([]tuple.Relationship, 0, number)
	for i := 0; i < number; i++ {
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: ONR(resourceName, fmt.Sprintf("%s-%d", resourceName, i), relation),
				Subject:  ONR(subjectName, fmt.Sprintf("%s-%d", subjectName, i), subjectRelation),
			},
		}
		rels = append(rels, rel)
	}

	return rels
}

func genRelsWithCaveat(resourceName string, relation string, subjectName string, subjectID string, caveatName string, context map[string]any, offset int, number int) []tuple.Relationship {
	return genRelsWithCaveatAndSubjectRelation(resourceName, relation, subjectName, subjectID, "...", caveatName, context, offset, number)
}

func genRelsWithCaveatAndSubjectRelation(resourceName string, relation string, subjectName string, subjectID string, subjectRelation string, caveatName string, context map[string]any, offset int, number int) []tuple.Relationship {
	rels := make([]tuple.Relationship, 0, number)
	for i := 0; i < number; i++ {
		rel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: ONR(resourceName, fmt.Sprintf("%s-%d", resourceName, i+offset), relation),
				Subject:  ONR(subjectName, subjectID, subjectRelation),
			},
		}

		if caveatName != "" {
			rel = tuple.MustWithCaveat(rel, caveatName, context)
		}
		rels = append(rels, rel)
	}
	return rels
}

func genResourceIds(resourceName string, number int) []string {
	resourceIDs := make([]string, 0, number)
	for i := 0; i < number; i++ {
		resourceIDs = append(resourceIDs, fmt.Sprintf("%s-%d", resourceName, i))
	}
	return resourceIDs
}

func TestLookupResourcesOverSchemaWithCursors(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                string
		schema              string
		relationships       []tuple.Relationship
		permission          tuple.RelationReference
		subject             tuple.ObjectAndRelation
		expectedResourceIDs []string
	}{
		{
			"basic union",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 1510),
				genRels("document", "editor", "user", "tom", 1510),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 1510),
		},
		{
			"basic exclusion",
			`definition user {}
		
		 	 definition document {
				relation banned: user
				relation viewer: user
				permission view = viewer - banned
  			 }`,
			genRels("document", "viewer", "user", "tom", 1010),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 1010),
		},
		{
			"basic intersection",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer & editor
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 510),
				genRels("document", "editor", "user", "tom", 510),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 510),
		},
		{
			"union and exclused union",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				relation banned: user
				permission can_view = viewer - banned
				permission view = can_view + editor
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 1310),
				genRelsWithOffset("document", "editor", "user", "tom", 1250, 1200),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 2450),
		},
		{
			"basic caveats",
			`definition user {}

 			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }
		
		 	 definition document {
				relation viewer: user with somecaveat
				permission view = viewer
  			 }`,
			genRelsWithCaveat("document", "viewer", "user", "tom", "somecaveat", map[string]any{"somecondition": 42}, 0, 2450),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 2450),
		},
		{
			"excluded items",
			`definition user {}
		
		 	 definition document {
				relation banned: user
				relation viewer: user
				permission view = viewer - banned
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 1310),
				genRelsWithOffset("document", "banned", "user", "tom", 1210, 100),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 1210),
		},
		{
			"basic caveats with missing field",
			`definition user {}

 			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }
		
		 	 definition document {
				relation viewer: user with somecaveat
				permission view = viewer
  			 }`,
			genRelsWithCaveat("document", "viewer", "user", "tom", "somecaveat", map[string]any{}, 0, 2450),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 2450),
		},
		{
			"larger arrow dispatch",
			`definition user {}
	
			 definition folder {
				relation viewer: user
			 }

		 	 definition document {
				relation folder: folder
				permission view = folder->viewer
  			 }`,
			joinTuples(
				genRels("folder", "viewer", "user", "tom", 150),
				genSubjectRels("document", "folder", "folder", "...", 150),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 150),
		},
		{
			"big",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			joinTuples(
				genRels("document", "viewer", "user", "tom", 15100),
				genRels("document", "editor", "user", "tom", 15100),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 15100),
		},
		{
			"all arrow",
			`definition user {}
		
			 definition folder {
				relation viewer: user
			 }

		 	 definition document {
			 	relation parent: folder
				relation viewer: user
				permission view = parent.all(viewer) + viewer
  			 }`,
			[]tuple.Relationship{
				tuple.MustParse("document:doc0#parent@folder:folder0"),
				tuple.MustParse("folder:folder0#viewer@user:tom"),

				tuple.MustParse("document:doc1#parent@folder:folder1-1"),
				tuple.MustParse("document:doc1#parent@folder:folder1-2"),
				tuple.MustParse("document:doc1#parent@folder:folder1-3"),
				tuple.MustParse("folder:folder1-1#viewer@user:tom"),
				tuple.MustParse("folder:folder1-2#viewer@user:tom"),
				tuple.MustParse("folder:folder1-3#viewer@user:tom"),

				tuple.MustParse("document:doc2#parent@folder:folder2-1"),
				tuple.MustParse("document:doc2#parent@folder:folder2-2"),
				tuple.MustParse("document:doc2#parent@folder:folder2-3"),
				tuple.MustParse("folder:folder2-1#viewer@user:tom"),
				tuple.MustParse("folder:folder2-2#viewer@user:tom"),

				tuple.MustParse("document:doc3#parent@folder:folder3-1"),

				tuple.MustParse("document:doc4#viewer@user:tom"),

				tuple.MustParse("document:doc5#viewer@user:fred"),
			},
			RR("document", "view"),
			ONR("user", "tom", "..."),
			[]string{"doc0", "doc1", "doc4"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, pageSize := range []int{0, 104, 1023} {
				pageSize := pageSize
				t.Run(fmt.Sprintf("ps-%d_", pageSize), func(t *testing.T) {
					t.Parallel()
					require := require.New(t)

					dispatcher := NewLocalOnlyDispatcher(10, 100)

					ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
					require.NoError(err)

					ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

					ctx := datastoremw.ContextWithHandle(context.Background())
					require.NoError(datastoremw.SetInContext(ctx, ds))

					var currentCursor *v1.Cursor
					foundResourceIDs := mapz.NewSet[string]()
					for {
						stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
						uintPageSize, err := safecast.ToUint32(pageSize)
						require.NoError(err)

						err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
							ObjectRelation: tc.permission.ToCoreRR(),
							Subject:        tc.subject.ToCoreONR(),
							Metadata: &v1.ResolverMeta{
								AtRevision:     revision.String(),
								DepthRemaining: 50,
							},
							OptionalLimit:  uintPageSize,
							OptionalCursor: currentCursor,
						}, stream)
						require.NoError(err)

						if pageSize > 0 {
							require.LessOrEqual(len(stream.Results()), pageSize)
						}

						for _, result := range stream.Results() {
							foundResourceIDs.Insert(result.ResolvedResource.ResourceId)
							currentCursor = result.AfterResponseCursor
						}

						if pageSize == 0 || len(stream.Results()) < pageSize {
							break
						}
					}

					foundResourceIDsSlice := foundResourceIDs.AsSlice()
					expectedResourceIDs := slices.Clone(tc.expectedResourceIDs)
					slices.Sort(foundResourceIDsSlice)
					slices.Sort(expectedResourceIDs)

					require.Equal(expectedResourceIDs, foundResourceIDsSlice)
				})
			}
		})
	}
}

func TestLookupResourcesImmediateTimeout(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10, 100)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	cctx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	require.NoError(datastoremw.SetInContext(cctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](cctx)

	err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view").ToCoreRR(),
		Subject:        ONR("user", "legal", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 10,
		},
	}, stream)

	require.ErrorIs(err, context.DeadlineExceeded)
	require.ErrorContains(err, "context deadline exceeded")
}

func TestLookupResourcesWithError(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10, 100)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	cctx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	require.NoError(datastoremw.SetInContext(cctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](cctx)

	err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view").ToCoreRR(),
		Subject:        ONR("user", "legal", "...").ToCoreONR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 1, // Set depth 1 to cause an error within reachable resources
		},
	}, stream)

	require.Error(err)
}
