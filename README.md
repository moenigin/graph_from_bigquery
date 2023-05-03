# graph_from_bigquery
This package provides functions to retrieve agglomeration graph information from
two BigQuery tables. It offers an alternative to the brainmaps_api_fcn package's
three EquivalenceRequest.functions: get_map, get_groups, and
get_equivalence_list. The BrainMapsAPI stores large, read-only agglomeration
graphs as a mapping of parent/agglomeration ID onto base segment ID
(representative representation), but it does not provide information about the
list of edges between pairs of base segments (source representation) that give
rise to a given agglomerated object/parent. To replace the brainmaps_api_fcn
functionality, this package constructs the agglomeration graph from two BigQuery
tables: one storing the representative mapping and another storing the source
representation.

## BigQueryAgglomerationGraph

With this class, you can:


* Retrieve the base segments agglomerated a given the supervoxel(s) sv_id
* Retrieve agglomerated id for a given base segment
* Retrieve the complete edge list of a segment in the agglomeration

The class has the following methods:

* 'query_src_edge_list(sv_id)': from bigquery table: retrieve the complete edge list of a segment in the original agglomeration with seg_id as src or targ
* 'query_supervoxel_members(sv_id, return_mapping=False)': retrieves the base segments agglomerated to the supervoxel(s) sv_id
* 'query_parent(sv_id, return_mapping=False)': retrieves agglomerated id for the base segments in sv_id
* 'get_map(sv_id)': for each segment in sv_id the id of agglomerated supervoxel it belongs to is returned
* 'get_groups(sv_id)': returns the list of all segments belonging to the same agglomerated supervoxel ids as the segment(s) in sv_id 
* 'get_equivalence_list(sv_id, verbose=False)': downloads list of all edges of segments in sv_id

You can create an instance of this class by providing the path to your service 
account file, the name of the representative table, and the name of the source 
table.


## BigQuery access
Authentication requires a service account json file enabling access to BigQuery 
tables

## Installation
This has only been tested using Python 3.10. 

```
pip install -r requirements.txt
```

## Usage Example
Retrieve id of the agglomerated object a given segment belongs to:
```
from google.cloud import bigquery
from google.oauth2 import service_account
from graph_from_bigquery import BigQueryAgglomerationGraph

# initialize the graph object
svc_acct_fpath = 'path/to/service_account.json'
representative_tbl = 'project_id.dataset.representative_table'
src_tbl = 'project_id.dataset.src_table'
bqag = BigQueryAgglomerationGraph(svc_acct_fpath, representative_tbl, src_tbl)

# query for the parents of a segment
segment_id = 55360714
parents = bqag.get_map(segment_id)
print(parents)
# Output: [1005144]

# query for parents of multiple segments
segment_ids = [55360714, 55360715, 55360716]
parents = bqag.get_map(segment_ids)
print(parents)
# Output e.g.: [1005144, 1005144, 1005145]
```

Get all members of the agglomerated object for base segment(s)
```
from google.cloud import bigquery
from google.oauth2 import service_account
from bigquery_agglomeration_graph import BigQueryAgglomerationGraph

# initialize the graph object
svc_acct_fpath = 'path/to/service_account.json'
representative_tbl = 'project_id.dataset.representative_table'
src_tbl = 'project_id.dataset.src_table'
bqag = BigQueryAgglomerationGraph(svc_acct_fpath, representative_tbl, src_tbl)

# query for groups of a segment
segment_id = 55360714
groups = bqag.get_groups(segment_id)
print(groups)
# Output: {55360714: [100005144, 55360714, 55360715, 55360716, 55360717, 55360718]}

# query for groups of multiple segments
segment_ids = [55360714, 55360715, 55360716]
groups = bqag.get_groups(segment_ids)
print(groups)
# Output: {55360714: [1005144, 55360714, 55360715, 55360717, 55360718],
#          55360715: [1005144, 55360714, 55360715, 55360717, 55360718],
#          55360716: [1005145, 55360716, 55360720, 55360721, 55360722, 55460750]}
```

Retrieve all edges in the agglomeration graph for base segment(s)
```
from google.cloud import bigquery
from google.oauth2 import service_account
from bigquery_agglomeration_graph import BigQueryAgglomerationGraph

# initialize the graph object
svc_acct_fpath = 'path/to/service_account.json'
representative_tbl = 'project_id.dataset.representative_table'
src_tbl = 'project_id.dataset.src_table'
bqag = BigQueryAgglomerationGraph(svc_acct_fpath, representative_tbl, src_tbl)

# query for the edges of a segment
segment_id = 55360714
edges = bqag.get_equivalence_list(segment_id)
print(edges)
# Output: [[55360714, 1005144], [55360714, 55360715], [55360714, 55360716], [55360714, 55360718]]
```