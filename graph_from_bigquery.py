from google.cloud import bigquery
from google.oauth2 import service_account

from datetime import timedelta
from timeit import default_timer as timer


def int_to_list(item):
    """Helper function to turn int input to list

    Args:
        item (int or list): Integer or List of integers

    Returns:
        list: List of integers
    Raises:
        ValueError: If the input is not an integer or a list
    """
    if isinstance(item, int):
        return [item]
    elif isinstance(item, list):
        return item
    else:
        raise ValueError("input needs to be integer or list, not", type(item))


def nested_set_to_list(set_):
    """Turns nested set into nested list

    Args:
        set_: A nested set

    Returns:
        list: A nested list
    """
    edge_list = [list(x) for x in set_]
    return edge_list


class BigQueryAgglomerationGraph():
    """
   Tool to retrieve agglomeration graph information via BigQuery Tables.

   This class allows to retrieve the edge information of agglomeration graphs
   from Bigquery and can replace the brainmaps_api_fcn package for read-only
   graphs that are only served in representative representation via the
   Brainmaps API

   Attributes:
       client (google.cloud.bigquery.client.Client): Instance of the BigQuery
           client object.
       representative_tbl (str): Table name for the representative graph.
       src_tbl (str): Table name for the source graph.
       report_time (bool): Flag that decides whether to print query durations.
       MAX_QUERY_LENGTH (int): Maximum query length allowed by BigQuery API.

   Methods:
       create_client(svc_acct_file):
           Creates a BigQuery Client from a service account json file.

       check_query_length(query, segment_ids):
           Checks whether query exceeds Google's maximum query length.

       chunk_query_str(query, segment_ids):
           Chunks the query string based on segment IDs.

       query_src_edge_list(sv_id):
           Retrieves the complete edge list of a segment in the original
           agglomeration with seg_id as src or targ.

       query_src_edge_list_agglo_objects(sv_id):
           Retrieves the complete edge list of a segments in the original
           agglomeration. ATTENTION: This function assumes all members of a given
           agglomerated object are provided in sv_id. It only queries the edges in
           which individual members of sv_id appear as source as such it will only
           return the full edge list of any given segment if also all its partners,
           i.e. members of the agglomerated object are queried.

       query_supervoxel_members(sv_id, return_mapping=False):
           Retrieves the base segments agglomerated to the supervoxel(s) sv_id.

       query_parent(sv_id, return_mapping=False):
           Retrieves agglomerated ID for the base segments in sv_id.

       get_map(sv_id):
           For each segment in sv_id the id of agglomerated supervoxel it belongs to
           is returned.

       get_groups(sv_id):
           Returns the list of all segments belonging to the same agglomerated
           supervoxel ids as the segment(s) in sv_id.

       get_equivalence_list(sv_id, multi_edge_count=False, whole_agglo_objects=False):
           Downloads list of all edges of segments in sv_id.

   """
    def __init__(self, svc_acct_fpath, representative_tbl, src_tbl,
                 report_time=False):
        """
        Initializes BigQueryAgglomerationGraph with input parameters.

        Args:
            svc_acct_fpath (str or pathlib.Path): Full file path of service account json.
            representative_tbl (str): Table name for the representative graph.
            src_tbl (str): Table name for the source graph.
            report_time (boolean): Flag that decides whether to print query durations.
        """
        self.client, credentials = self.create_client(svc_acct_fpath)
        self.representative_tbl = '.'.join(
            [credentials.project_id, representative_tbl])
        self.src_tbl = '.'.join([credentials.project_id, src_tbl])
        self.report_time = report_time
        self.MAX_QUERY_LENGTH = 1024000


    @staticmethod
    def create_client(svc_acct_file):
        """Creates a bigquery.Client from a service account json file

        Args:
            svc_acct_file (str or pathlib.Path): Full file name of service account json

        Returns:
            tuple: A tuple of BigQuery Client and Credentials
        """
        credentials = service_account.Credentials.from_service_account_file(
            svc_acct_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        client = bigquery.Client(credentials=credentials,
                                 project=credentials.project_id, )
        return client, credentials


    def check_query_length(self, query, segment_ids):
        """Checks whether query exceeds Google's maximum query length.

        Args:
            query (str): Query string.
            segment_ids (int or list): segment ids to query
        """
        query_str = query.replace('#', ','.join([str(x) for x in segment_ids]))
        if len(query_str) <= self.MAX_QUERY_LENGTH:
            return [query_str]
        else:
            return None

    def chunk_query_str(self, query, segment_ids):
        """Chunks the query string based on segment IDs

        Args:
            query (str): Query string
            segment_ids (int or list): Segment IDs

        Returns:
            list: A list of chunked queries
        """
        # define
        segment_ids = int_to_list(segment_ids)
        n_segments = len(segment_ids)

        query_str = self.check_query_length(query, segment_ids)
        if query_str is not None:
            return [query_str]

        start = timer()
        chunked_queries = []
        while len(segment_ids) > 0:
            seg_str = ''
            query_str = ''
            while len(query_str) <= self.MAX_QUERY_LENGTH:
                seg_str = seg_str + str(int(segment_ids[0])) + ','
                query_str = query.replace('#', seg_str[:-1])
                segment_ids.pop(0)
                if len(segment_ids) == 0:
                    break
            chunked_queries.append(query_str)
        stop = timer()
        if self.report_time:
            print('making query string for', n_segments, 'segments in',
                  len(chunked_queries), 'chunks took',
                  timedelta(seconds=stop - start))

        return chunked_queries

    def query_src_edge_list(self, sv_id):
        """Retrieves the complete edge list of a segment in the original
         agglomeration with seg_id as src or targ

        Args:
            sv_id (int or list): Segment ID or list of Segment IDs for which to
                                 query the associated base segments

        Returns:
            list: A list of edges
        """
        QUERY = 'SELECT id1, id2 FROM `{}` WHERE id1 IN (#) OR ' \
                'id2 IN (#)'.format(self.src_tbl)

        queries = self.chunk_query_str(QUERY, sv_id)
        results = []
        for i, query in enumerate(queries):
            start = timer()
            query_job = self.client.query(query)
            rows = query_job.result()
            stop = timer()
            if self.report_time:
                print('making query  for', i, 'of', len(queries),
                      'chunks took', timedelta(seconds=stop - start))

            for row in rows:
                edge = frozenset([int(row.id1), int(row.id2)])
                results.append(edge)
        return results

    def query_src_edge_list_agglo_objects(self, sv_id):
        """Retrieves the complete edge list of a segments in the original
         agglomeration.
         ATTENTION: This function assumes all members of a given agglomerated
                    object are provided in sv_id. It only queries the edges in
                    which individual members of sv_id appear as source as such
                    it will only return the full edge list of any given segment
                    if also all its partners, ie. members of the agglomerated
                    object are queried.


        Args:
            sv_id (int or list): Segment ID or list of Segment IDs for which to
                                 query the associated base segments

        Returns:
            list: A list of edges
        """
        QUERY = 'SELECT id1, id2 FROM `{}` WHERE id1 IN (#)'.format(self.src_tbl)

        queries = self.chunk_query_str(QUERY, sv_id)
        results = []
        for i, query in enumerate(queries):
            start = timer()
            query_job = self.client.query(query)
            rows = query_job.result()
            stop = timer()
            if self.report_time:
                print('making query  for', i, 'of', len(queries),
                      'chunks took', timedelta(seconds=stop - start))

            for row in rows:
                edge = frozenset([int(row.id1), int(row.id2)])
                results.append(edge)
        return results


    def query_supervoxel_members(self, sv_id, return_mapping=False):
        """Retrieves the base segments agglomerated to the supervoxel(s) sv_id

        Args:
            sv_id (int or list): Segment ID or list of Segment IDs for which to
                                 query the associated base segments
            return_mapping (boolean): Flag determining whether to return mapping
                                      of supervoxel ID(s) sv_id on base segment
                                      ID or only the base segments

        Returns:
            list: A list of base segments or mapping of supervoxel ID(s) sv_id
                  on base segment ID
        """
        if return_mapping:
            QUERY = 'SELECT id_a, id_b FROM `{}` WHERE id_b IN (#)'.format(
                self.representative_tbl)
        else:
            QUERY = 'SELECT id_a FROM `{}` WHERE id_b IN (#)'.format(
                self.representative_tbl)

        queries = self.chunk_query_str(QUERY, sv_id)
        results = []
        for i, query in enumerate(queries):
            start = timer()
            query_job = self.client.query(query)
            rows = query_job.result()
            stop = timer()
            if self.report_time:
                print('making query  for', i, 'of', len(queries),
                      'chunks took', timedelta(seconds=stop - start))
            for row in rows:
                if return_mapping:
                    results.append(tuple([row.id_a, row.id_b]))
                else:
                    results.append(row.id_a)
        return results

    def query_parent(self, sv_id, return_mapping=False):
        """Retrieves agglomerated ID for the base segments in sv_id

        Args:
            sv_id (int or list): Segment ID or list of Segment IDs for which to
                                query the associated base segments
            return_mapping (boolean): Flag determining whether to return mapping
                                      of supervoxel ID(s) sv_id on base segment
                                      ID or only the base segments

        Returns:
            list: A list of agglomerated segment IDs or mapping of supervoxel
                  ID(s) sv_id on base segment ID
        """
        if return_mapping:
            QUERY = 'SELECT id_a, id_b FROM `{}` WHERE id_a IN (#)'.format(
                self.representative_tbl)
        else:
            QUERY = 'SELECT id_b FROM `{}` WHERE id_a IN (#)'.format(
                self.representative_tbl)

        queries = self.chunk_query_str(QUERY, sv_id)
        results = []
        for i, query in enumerate(queries):
            start = timer()
            query_job = self.client.query(query)
            rows = query_job.result()
            stop = timer()
            if self.report_time:
                print('making query  for', i, 'of', len(queries),
                      'chunks took', timedelta(seconds=stop - start))
            for row in rows:
                if return_mapping:
                    results.append(tuple([row.id_a, row.id_b]))
                else:
                    results.append(row.id_b)

        return results

    def get_map(self, sv_id):
        """For each segment in sv_id the id of agglomerated supervoxel it
        belongs to is returned.

        Args:
            sv_id (int or list): supervoxel id(s)

        Returns:
            list: list of agglomerated segment ids
        """
        sv_id = int_to_list(sv_id)
        results = self.query_parent(sv_id, return_mapping=True)
        maps = {child: parent for child, parent in results}

        parent_list = []
        # if a given segment is not agglomerated or is the lowest id in the
        # agglomerated supervoxel it will not be retrieved from the bigquery and
        # has to be entered to the list separately
        for sv in sv_id:
            if sv in maps.keys():
                parent_list.append(maps[sv])
            else:
                parent_list.append(sv)
        return parent_list

    def get_groups(self, sv_id):
        """Returns the list of all segments belonging to the same agglomerated
        supervoxel ids as the segment(s) in sv_id.

        Args:
            sv_id (int or list): supervoxel ids

        Returns:
            dict: maps each entry in sv_id onto a list of members of the
                agglomerated object
                {sv1: [sv1, sv2, sv3,...],
                sv11: [sv11,sv12,sv13,...],
                ...
                }
        """
        sv_id = int_to_list(sv_id)
        # for each segment in sv_id first retrieve the parent agglomeration id
        # and then retrieve the associated group members
        parents = self.get_map(sv_id)
        unique_parents = list(set(parents))
        results = self.query_supervoxel_members(unique_parents,
                                                return_mapping=True)
        representative_graph = {child: parent for child, parent in results}

        mapping = dict()
        for c, p in representative_graph.items():
            mapping[p] = [c] if p not in mapping.keys() else mapping[p] + [c]

        members = dict()
        for seg, p in zip(sv_id, parents):
            if p in mapping.keys():
                members[seg] = [p] + mapping[p]
            else:
                if seg != p:
                    raise ValueError("This should not happen: "
                                     "parent {} not retrieved for segment {}".format(
                        p, seg))
                members[seg] = [seg]
        return members

    def get_equivalence_list(self, sv_id, multi_edge_count=False,
                             whole_agglo_objects=False):
        """Downloads list of all edges of segments in sv_id

        Returns a list containing all edges of the supervoxels in sv_id. Edges
        between members of sv_id will only appear once ("undirected"). The edge
        list return is not sorted by sv_id entry.

        Args:
            sv_id (int or list): segment ids
            multi_edge_count (bool, optional): Whether to count the number of
                                               edges that appear multiple times
                                               during agglomeration. Default
                                               is False.
            whole_agglo_objects (bool, optional): Whether to retrieve the edge
                                                  list for the entire agglomerated
                                                  object of which the segments in
                                                  sv_id are part of. Note: Only
                                                  use this option if multi_edge_count
                                                  is set to True. Default is False.

        Returns:
            list: List with all edges of segments in sv_id

            If multi_edge_count=True and whole_agglo_objects=False, returns a
            tuple containing two elements:
            - list: List with all edges of segments in sv_id
            - dict: Dictionary containing the edges that appear multiple times
                    during agglomeration and the number of times they appear
                    in the queried edge list.

            If whole_agglo_objects=True, returns only the list of edges.

            Note: If multi_edge_count=True and whole_agglo_objects=True, a
            ValueError is raised since this combination is not supported.
        """
        def document_multiple_edges(multiple_edges, edge):
            """helper function to count the number of times a given pair of
            segments has crossed threshold for merge decision during
            agglomeration"""
            if edge in multiple_edges.keys():
                multiple_edges[edge] = multiple_edges[edge] + 1
            else:
                multiple_edges[edge] = 2
            return multiple_edges

        if multi_edge_count and not whole_agglo_objects:
            raise ValueError('This is not supported. The number of multiple '
                             'edges cannot be reliably estimated for large '
                             'queries unless the edge list of the whole '
                             'agglomerated objects is queried')
        if whole_agglo_objects:
            results = self.query_src_edge_list_agglo_objects(sv_id)

            multiple_edges = dict()
            edge_set = set()
            for e_set in results:
                if len(e_set) != 2:
                    continue
                if multi_edge_count:
                    if e_set in edge_set:
                        multiple_edges = document_multiple_edges(multiple_edges, e_set)
                edge_set.add(e_set)

            edges = nested_set_to_list(edge_set)
            if multi_edge_count:
                return edges, multiple_edges
        else:
            results = self.query_src_edge_list(sv_id)

            edge_set = set()
            for e_set in results:
                if len(e_set) != 2:
                    continue
                edge_set.add(e_set)

            edges = nested_set_to_list(edge_set)
        return edges