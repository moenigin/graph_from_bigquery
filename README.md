# graph_from_bigquery

Functions to retrieve agglomeration graph information from two BigQuery tables. 
This package imitates the three EquivalenceRequest.functions get_map, get_group 
and get_equivalence_list of the package brainmaps_api_fcn. The BrainMapsAPI 
stores large, read-only agglomeration graphs only in a mapping of 
parent/agglomeration ID onto base segment ID (=representative representation) 
but does not provide about the list of edges between pairs of base segments 
(=source representation) that give rise to a given agglomerated object/parent. 
To replace the brainmaps_api_fcn functionality the agglomeration graph is 
constructed from 2 Bigquery tables - one storing the represenatative mapping and 
one storing the source representation.   


## BigQuery access
Authentication requires a service account json file enabling access to BigQuery 
tables

## Installation
This has only been tested using Python 3.10. 

```
pip install -r requirements.txt
```

## Usage Example
Retrieve connected segments from an agglomeration graph
```
from brainmaps_api_fcn.equivalence_requests import EquivalenceRequests
er = EquivalenceRequests(<path_to_client_secret>, volume_id, change_stack_id)
segment_id = 55360714
edges = er.get_equivalence_list(segment_id)
```
