import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import psycopg2
import StringIO
import networkx as nx
import traceback
from kafka import KafkaProducer
from datetime import datetime
import json
from pymongo import MongoClient

#this code version needs to do: 
#1. group creation and report 
#       -- using both local and global neighbors, in the message report fault asset and their streets
#2. the number of faults with the same error type in the street > 30%, report 30% error message         
#       -- need to discuss the message format for 30% error, if new fault in the street comes, to report again? if fault closes, report 30% error closes?
#3. use both local and global neighbors to create graph and perform fault group detection

#group events:
#1. group create
#2. group close
#3. group update
#4. group merge
#5. group split

#group table schema:
#Group_id, Asset_id, fault_id, error_key, is_closed, last_updated_time


#the fault record processing steps:
#1. insert into faults table
#2. get the fault error_key, asset_id, closed_on time from table, 
#        if closed_on time is not None, the fault is closed, possible group events:
#               -- group close
#               -- group update
#               -- group split
#3. get the fault asset street_id and street_name
#4. get all assets and their local neighbors from asset_neighbors table
#5. create a graph for the street assets
#6. get all open faults with the same error_key on the street
#7. compute groups on the fault sub-graph
#8. if the new fault belongs to a group, report it, possible group events:
#               -- group create
#               -- group update
#               -- group merge

#Is it possible only fault update, not new fault, not fault close?      -- we don't need to do anything for update events, update events don't influence fault groups 
#and if a fault update can change error_key?    -- assuming fault update cannot change error_key

#try to divide logic into new fault, fault close and fault update events
#try to put code into methods, to make the code cleaner

#how to debug the code, if I changed it?
#set up a dataset, so I can test        -- use Barcelona dataset, the data is in fault_test database


class FaultStreamProcessor:
        def __init__(self, fault_max_ratio, num_group_faults_limit, street_min_num_assets_to_report):
                self.fault_max_ratio = fault_max_ratio
                self.num_group_faults_limit = num_group_faults_limit
                self.street_min_num_assets_to_report = street_min_num_assets_to_report
                
                #the fault group id serial, starting from 1
                self.group_id = 1    
                
                with open('../config/config.json') as config_file:    
                    config_data = json.load(config_file)
                    
                self.pg_dbname = config_data['pg_dbname']
                self.pg_username = config_data['pg_username']
                self.pg_password = config_data['pg_password']
                self.pg_host = config_data['pg_host']
                self.pg_port = config_data['pg_port']
                self.mongo_dbname = config_data['mongo_dbname']
                self.mongo_collection_name = config_data['mongo_collection_name']
                            
                
                
        def graphCreate(self, conn, cur, street_id, street_name):
                #use the asset local neighbors and global neighbors in the street to create street graph
                #input:
                #       conn:  this is the database connection
                #       cur:   this is the database cursor                         
                #       street_id:      the street id 
                #       street_name:    the street name
                
                #create a graph for assets in the street
                self.G=nx.Graph()
                
                #********** add local neighbors to the graph **********
                                
                #get all assets and their two local neighbors in the street
                cur.execute("select street_id, asset_id, first_neighbor_id, second_neighbor_id from asset_neighbors where street_id = %s", (street_id,))                        
                rows = cur.fetchall()
                
                #the number of assets in this street
                self.num_assets_in_street = len(rows)

                #asset and their neighbor list
                asset_id_list = []
                neighbor1_list = []
                neighbor2_list = []

                for row in rows:
                        asset_id_list.append(int(row[1]))
                        neighbor1_list.append(int(row[2]))
                        neighbor2_list.append(int(row[3]))

                #add all assets in the street as nodes
                self.G.add_nodes_from(asset_id_list)

                num_nodes = len(asset_id_list)
                #create edges 
                edges = []
                #collect all edges according to the neighbors information
                for i in range(num_nodes):
                        edges.append((asset_id_list[i], neighbor1_list[i]))
                        edges.append((asset_id_list[i], neighbor2_list[i]))

                #add all edges to the graph        
                self.G.add_edges_from(edges)    
                
                #this only includes asset in the street
                #the value is a list 
                self.asset_lists_in_street = self.G.nodes()
                
                
                #********** add global neighbors to the graph **********
                  
                #get all assets and their two global neighbors in the street
                cur.execute("select street_id, asset_id, first_neighbor_id, second_neighbor_id from global_asset_neighbors where street_id = %s", (street_id,))                        
                rows = cur.fetchall()
                
                #asset and their neighbor list
                global_asset_id_list = []
                global_neighbor1_list = []
                global_neighbor2_list = []
                
                for row in rows:
                        global_asset_id_list.append(int(row[1]))
                        global_neighbor1_list.append(int(row[2]))
                        global_neighbor2_list.append(int(row[3]))
                
                #add all assets in the street as nodes
                self.G.add_nodes_from(global_asset_id_list)
                
                num_nodes = len(global_asset_id_list)
                #create edges 
                edges = []
                #collect all edges according to the neighbors information
                for i in range(num_nodes):
                        edges.append((global_asset_id_list[i], global_neighbor1_list[i]))
                        edges.append((global_asset_id_list[i], global_neighbor2_list[i]))

                #add all edges to the graph        
                self.G.add_edges_from(edges)    
                
                #this includes all nodes in the graph, some global neighbors may be from other streets  
                #the value is a list              
                self.all_graph_nodes = self.G.nodes()
                #total number of nodes in the graph
                self.num_nodes_in_the_graph = len(self.all_graph_nodes)
                               
                        
        def groupCreateAddFault(self, conn, cur, producer, mongoCollection, comp, error_key):
                #this is the group create method
                #we need to: 
                #       1. insert records into 'fault_groups' table
                #       2. send JSON message
                #       3. append JSON document to MongoDB
                                
                current_time = datetime.now().replace(microsecond=0)
                
                fault_id_list = []
                #****** 1. insert each record in the group to the fault_groups table ******
                for comp_asset_id in comp:
                        #get the fault id for each asset in the component
                        cur.execute("select id from faults where asset_id=%s and error_key=%s and closed_on is null", (comp_asset_id, error_key))  
                        fault_id_row = cur.fetchone()
                        if fault_id_row is not None:
                                fault_id_list.append(fault_id_row[0])
                                #insert the group asset record into the fault_groups table
                                cur.execute("insert into fault_groups (group_id, asset_id, fault_id, error_key, is_closed, last_updated_time) values (%s, %s, %s, %s, %s, %s)", (self.group_id, comp_asset_id, fault_id_row[0], error_key, False, current_time))
                
                #commit the update in the fault_groups table
                conn.commit()
                
                #****** 2. construct and send the JSON object message ******
                
                #construct the json object
                json_obj = {}
                json_obj['Action'] = 'Group created'
                json_obj['Group_id'] = [self.group_id]
                json_obj['Fault_assets'] = list(comp)
                json_obj['Fault_id'] = fault_id_list
                json_obj['Error_key'] = error_key
                json_obj['Time'] = str(current_time)
                
                #convert the json object to string 
                json_message = json.dumps(json_obj)
                producer.send('test-python-kafka', json_message)
                               
                #****** 3. append the JSON object to MongoDB ******
                #see how to create connection to MongoDB, how to insert JSON documents into MongoDB
                
                #add the json_obj into MongoDB 
                mongoCollection.insert_one(json_obj)
                
                #increment the global group_id
                self.group_id = self.group_id + 1
                                
                        
                
        def groupUpdateAddFault(self, conn, cur, producer, mongoCollection, group_id, fault_id, asset_id, error_key):
                #this is the group update add fault method
                #we need to:
                #       1. add the new fault with the group_id into the 'fault_groups' table
                #       2. update the last_updated_time for other assets in the group
                #       3. send JSON message
                #       4. append JSON document to MongoDB
                                
                current_time = datetime.now().replace(microsecond=0)
                
                #update the last_updated_time for other assets in the group
                cur.execute("update fault_groups set last_updated_time=%s where group_id=%s and is_closed='f'", (current_time, group_id))
                
                #get fault_id for this asset
                #cur.execute("select id from faults where asset_id=%s and error_key=%s and closed_on is null", (asset_id, error_key))  
                #fault_id_row = cur.fetchone()
                #if fault_id_row is not None:                
                
                #insert the new fault asset with the group_id into the 'fault_groups' table
                cur.execute("insert into fault_groups (group_id, asset_id, fault_id, error_key, is_closed, last_updated_time) values (%s, %s, %s, %s, %s, %s)", (group_id, asset_id, fault_id, error_key, False, current_time))
                
                #commit the update in the fault_groups table
                conn.commit()
                
                #****** 2. construct and send the JSON object message ******
                
                #get all asset id in the group
                cur.execute("select asset_id from fault_groups where group_id=%s and is_closed='f'", (group_id,))
                asset_id_rows = cur.fetchall()
                
                group_asset_id_list = []
                for row in asset_id_rows:
                        group_asset_id_list.append(row[0])
                
                #construct the json object
                json_obj = {}
                json_obj['Action'] = 'Group updated'
                json_obj['Group_id'] = [group_id]
                json_obj['Fault_assets'] = group_asset_id_list
                json_obj['added_assets'] = [asset_id]
                json_obj['removed_assets'] = []
                json_obj['Error_key'] = error_key
                json_obj['Time'] = str(current_time)
                        
                #convert the json object to string 
                json_message = json.dumps(json_obj)
                producer.send('test-python-kafka', json_message)
                
                #****** 3. append the JSON object to MongoDB ******
                #see how to create connection to MongoDB, how to insert JSON documents into MongoDB
                
                #add the json_obj into MongoDB 
                mongoCollection.insert_one(json_obj)
                
                                        
        
        def groupMergeAddFault(self, conn, cur, producer, mongoCollection, group_id_list, fault_id, asset_id, error_key):
                #this is the group merge method
                #input:  group_id_list is a list of group id to be merged, 
                #              fault_id, asset_id, error_key is for the new fault asset 
                #we need to:
                #       1. update the 'fault_groups' table, merge other groups into the group with the smallest group_id, close other groups
                #       2. insert the new fault asset into the group with the smallest group_id in 'fault_groups' table
                #       3. when updating, change the last_updated_time for all updated records in the 'fault_groups' table 
                #       4. send JSON message
                #       5. append JSON document to MongoDB
                
                current_time = datetime.now().replace(microsecond=0) 
                                                
                #list of group asset lists
                asset_groups = [] 
                
                #get merged group assets as a list of list
                for group_id in group_id_list:
                        #get asset id in the group
                        cur.execute("select asset_id from fault_groups where group_id=%s and is_closed='f'", (group_id,))
                        asset_id_rows = cur.fetchall()
                        group_asset_id_list = []
                        for row in asset_id_rows:
                                group_asset_id_list.append(row[0])
                        #add the list to asset_groups, which is a list of list        
                        asset_groups.append(group_asset_id_list)
                
                #get the smallest group id, use this group id as the merged group id
                min_group_id = min(group_id_list)
                
                #set other merged group id to the min_group_id
                for group_id in group_id_list:                        
                        
                        if group_id != min_group_id:
                                #update the group id to min_group_id
                                cur.execute("update fault_groups set group_id=%s where group_id=%s and is_closed='f'", (min_group_id, group_id))
                                
                #update the last_updated_time for all assets in the group
                cur.execute("update fault_groups set last_updated_time=%s where group_id=%s and is_closed='f'", (current_time, min_group_id))   
                                
                #insert the new fault asset 
                cur.execute("insert into fault_groups (group_id, asset_id, fault_id, error_key, is_closed, last_updated_time) values (%s, %s, %s, %s, %s, %s)", (min_group_id, asset_id, fault_id, error_key, False, current_time))              
                
                #commit the update in the fault_groups table
                conn.commit()
                
                #****** 2. construct and send the JSON object message ******
                
                #construct the json object
                json_obj = {}
                json_obj['Action'] = 'Group merged'
                json_obj['Group_id'] = [min_group_id]
                json_obj['Merged_groups'] = group_id_list
                json_obj['Merged_assets'] = asset_groups
                json_obj['new_fault_assets'] = [asset_id]
                json_obj['Error_key'] = error_key
                json_obj['Time'] = str(current_time)
                
                #convert the json object to string 
                json_message = json.dumps(json_obj)
                producer.send('test-python-kafka', json_message)
                
                #****** 3. append the JSON object to MongoDB ******
                #see how to create connection to MongoDB, how to insert JSON documents into MongoDB
                
                #add the json_obj into MongoDB 
                mongoCollection.insert_one(json_obj)
                
                                
                
        def faultAddGroupOperation(self, conn, cur, producer, mongoCollection, fault_id, asset_id, comp, error_key):
                #the group operation for fault add event
                #       including group create, group update, group merge
                #input:  conn, cur are db connection and cursor, producer is the kafka producer
                #       fault_id is the new fault id
                #       asset_id is the new fault asset id
                #       comp is the set of asset id in the component
                #       error_key is the error_key for the new fault asset
                
                cur.execute("select group_id from fault_groups where asset_id=%s and error_key=%s and is_closed='f'", (asset_id, error_key)) 
                fault_group_row = cur.fetchone()
                
                if fault_group_row is not None:
                        #this new fault asset has already existed in a group
                        #no need to do any group operation
                        return
                
                #the group id set for assets in the component
                group_id_set = set()
                
                for comp_asset_id in comp:
                        #check each asset inside the component
                        if comp_asset_id == asset_id:
                                continue
                        cur.execute("select group_id from fault_groups where asset_id=%s and error_key=%s and is_closed='f'", (comp_asset_id, error_key)) 
                        fault_group_row = cur.fetchone()
                        
                        if fault_group_row is not None:
                                #add the group_id for the asset to the set        
                                group_id_set.add(fault_group_row[0])
                                
                group_id_set_size = len(group_id_set)
                
                if group_id_set_size == 0:
                        #we need to create group 
                        self.groupCreateAddFault(conn, cur, producer, mongoCollection, comp, error_key)
                        
                elif group_id_set_size == 1:
                        #we need to update group
                        group_id = list(group_id_set)[0]
                        #update the group and add the new fault asset
                        self.groupUpdateAddFault(conn, cur, producer, mongoCollection, group_id, fault_id, asset_id, error_key)
                else:
                        #group_id_set_size > 1
                        #we need to merge groups
                        group_id_list = list(group_id_set)
                        self.groupMergeAddFault(conn, cur, producer, mongoCollection, group_id_list, fault_id, asset_id, error_key)                                        
                        
                
        def faultAddEventProcess(self, conn, cur, producer, mongoCollection, fault_id, asset_id, error_key, closed_on_time, street_id, street_name):
                #processing for fault add event
                #possible group operation:      group create, group update, group merge
                
                #use both local street neighbors and global neighbors to create the neighbor graph                                
                self.graphCreate(conn, cur, street_id, street_name)
                
                #get all open faults with the given error_key
                cur.execute("select id, error_key, is_open, first_reported_on, asset_id, closed_on from faults where error_key=%s and closed_on is null", (error_key,))  
                rows = cur.fetchall()

                #the number of faults in the list
                #num_faults = len(rows)
                
                #fault asset id in the current time
                fault_asset_id_list = []

                for row in rows:
                        fault_asset_id_list.append(row[4])
                                        
                #******* check 30% error message in the street ******                
                """
                fault_asset_id_in_the_street = [id for id in fault_asset_id_list if id in self.asset_lists_in_street]

                if self.num_assets_in_street >= self.street_min_num_assets_to_report and len(fault_asset_id_in_the_street) > self.fault_max_ratio * self.num_assets_in_street:				
                        #print "The number of faults in the street is more than %f of the total number of assets in the street." % (fault_max_ratio,)
                        #print "fault: %d, on street %s, causes more than %f of all the assets in the street fault." % (fault_id, street_name, fault_max_ratio)
                        msg = "Street %s (with %d assets in total), has more than %d%% of all the assets with %s fault in the street ." % (street_name, self.num_assets_in_street, int(self.fault_max_ratio*100), error_key)
                        producer.send('test-python-kafka', msg)
        		#f.write("fault: %d, on street %s, causes more than %f of all the assets in the street fault.\n" % (fault_id, street_name, fault_max_ratio))
                """
                
                #******* fault group operation ******  
                
                #fault asset subgraph connected component size detection
                fault_subgraph_nodes = [id for id in fault_asset_id_list if id in self.all_graph_nodes]              
                        
                G_fault = self.G.subgraph(fault_subgraph_nodes)

                #print G_fault.nodes()
                #print G_fault.edges()

                for comp in nx.connected_components(G_fault):
                        #comp is a set of asset id within each connected component
                        comp_size = len(comp)                        
                        
                        if asset_id in comp and comp_size >= self.num_group_faults_limit:
                                #the component, that the new fault asset belongs to, forms a group
                                self.faultAddGroupOperation(conn, cur, producer, mongoCollection, fault_id, asset_id, comp, error_key)
                                        
                                #msg = "Street %s (with %d assets in total), has %d continuous fault assets with error_key %s in the street ." % (street_name, self.num_assets_in_street, comp_size, error_key)
                                #producer.send('test-python-kafka', msg)
        			
        
        def groupCloseDeleteFault(self, conn, cur, producer, mongoCollection, group_id, error_key):
                #this is the group close method
                #we need to: 
                #       1. set 'is_closed' to true for the records in the group in 'fault_groups' table
                #       2. update the 'last_updated_time' for records in the group
                #       3. send JSON message
                #       4. append JSON document to MongoDB
                                
                current_time = datetime.now().replace(microsecond=0)
                
                #get asset_id list and fault_id list for assets in the group, we will use the information in the message construction
                cur.execute("select asset_id, fault_id from fault_groups where group_id=%s and is_closed='f'", (group_id,))
                asset_rows = cur.fetchall()
                
                asset_id_list = []
                fault_id_list = []
                for row in asset_rows:
                        asset_id_list.append(row[0])
                        fault_id_list.append(row[1])
                        
                #update the last_updated_time and is_closed for all open assets in the group
                #close the group
                cur.execute("update fault_groups set is_closed=%s, last_updated_time=%s where group_id=%s and is_closed='f'", (True, current_time, group_id))   
                        
                #commit the update in the fault_groups table
                conn.commit()
                
                #****** 2. construct and send the JSON object message ******
                
                #construct the json object
                json_obj = {}
                json_obj['Action'] = 'Group closed'
                json_obj['Group_id'] = [group_id]
                json_obj['Fault_assets'] = asset_id_list
                json_obj['Fault_id'] = fault_id_list
                json_obj['Error_key'] = error_key
                json_obj['Time'] = str(current_time)
                
                #convert the json object to string 
                json_message = json.dumps(json_obj)
                producer.send('test-python-kafka', json_message)
                
                
                #****** 3. append the JSON object to MongoDB ******
                #see how to create connection to MongoDB, how to insert JSON documents into MongoDB
                
                #add the json_obj into MongoDB 
                mongoCollection.insert_one(json_obj)
                
                
                
        def groupUpdateDeleteFault(self, conn, cur, producer, mongoCollection, group_id, asset_id, error_key):
                #this is group update method for fault delete event
                #we need to:
                #       1. update the 'last_updated_time' for open records in the group
                #       2. set 'is_closed' to true only for the input closed asset
                #       3. send JSON message
                #       4. append JSON document to MongoDB
                
                current_time = datetime.now().replace(microsecond=0)
                 
                #update the last_updated_time for all open assets in the group
                cur.execute("update fault_groups set last_updated_time=%s where group_id=%s and is_closed='f'", (current_time, group_id))   
                
                #set is_closed to true for the input closed assets
                #update the group
                cur.execute("update fault_groups set is_closed=%s where group_id=%s and asset_id=%s and is_closed='f'", (True, group_id, asset_id)) 
                
                #commit the update in the fault_groups table
                conn.commit()
                
                #get the remaining open asset_id list in the group, we will use the information in the message construction
                cur.execute("select asset_id from fault_groups where group_id=%s and is_closed='f'", (group_id,))
                asset_rows = cur.fetchall()
                
                asset_id_list = []
                for row in asset_rows:
                        asset_id_list.append(row[0])
                
                  
                #****** 2. construct and send the JSON object message ******
                
                #construct the json object
                json_obj = {}
                json_obj['Action'] = 'Group updated'
                json_obj['Group_id'] = [group_id]
                json_obj['Fault_assets'] = asset_id_list
                json_obj['added_assets'] = []
                json_obj['removed_assets'] = [asset_id]
                json_obj['Error_key'] = error_key
                json_obj['Time'] = str(current_time)
                
                #convert the json object to string 
                json_message = json.dumps(json_obj)
                producer.send('test-python-kafka', json_message)
                
                #****** 3. append the JSON object to MongoDB ******
                #see how to create connection to MongoDB, how to insert JSON documents into MongoDB
                
                #add the json_obj into MongoDB 
                mongoCollection.insert_one(json_obj)
                
                
                
        def groupSplitDeleteFault(self, conn, cur, producer, mongoCollection, group_id, asset_id, error_key, G_fault, group_asset_set_except_deleted_one): 
                #this is group split method
                #we need to:
                #       1. update the 'last_updated_time' for open records in the group
                #       2. set 'is_closed' to true for the input closed asset   
                #       3. update the group id for other open assets in the group according to fault group detection result, the group id is newly generated
                #       4. send JSON message
                #       5. append JSON document to MongoDB
                     
                current_time = datetime.now().replace(microsecond=0)
                
                #update the last_updated_time for all open assets in the group
                cur.execute("update fault_groups set last_updated_time=%s where group_id=%s and is_closed='f'", (current_time, group_id))   
                
                #set is_closed to true for the input closed assets
                cur.execute("update fault_groups set is_closed=%s where group_id=%s and asset_id=%s and is_closed='f'", (True, group_id, asset_id)) 
                
                #after split, the newly generated group id list
                new_group_id_list = []
                #group assets
                group_assets_lists = []
                #generate new groups for other assets in the group
                #split the group
                for comp in nx.connected_components(G_fault):
                        #comp is a set of asset id within each connected component
                        comp_size = len(comp)                        
                        set_intersection = group_asset_set_except_deleted_one & comp
                        
                        if len(set_intersection)>0 and comp_size >= self.num_group_faults_limit:
                                #the asset component forms a group
                                new_group_id_list.append(self.group_id)
                                
                                asset_list = list(comp)
                                group_assets_lists.append(asset_list)
                                
                                for comp_asset_id in comp:
                                        #update the group id for assets in the component
                                        cur.execute("update fault_groups set group_id=%s where group_id=%s and asset_id=%s and is_closed='f'", (self.group_id, group_id, comp_asset_id)) 
                                           
                                #increment the global group_id
                                self.group_id = self.group_id + 1
                
                #commit the update in the fault_groups table
                conn.commit()
                
                #****** 2. construct and send the JSON object message ******
                
                #construct the json object
                json_obj = {}
                json_obj['Action'] = 'Group split'
                json_obj['Group_id'] = [group_id]
                json_obj['Split_groups'] = new_group_id_list
                json_obj['Group_assets'] = group_assets_lists
                json_obj['Error_key'] = error_key
                json_obj['Time'] = str(current_time)
                
                #convert the json object to string 
                json_message = json.dumps(json_obj)
                producer.send('test-python-kafka', json_message)
                
                #****** 3. append the JSON object to MongoDB ******
                #see how to create connection to MongoDB, how to insert JSON documents into MongoDB
                
                #add the json_obj into MongoDB 
                mongoCollection.insert_one(json_obj)
                
                
        
        def faultDeleteEventProcess(self, conn, cur, producer, mongoCollection, fault_id, asset_id, error_key, closed_on_time, street_id, street_name):
                #processing for fault delete event
                #possible group operation:      group close, group update, group split
                #input:   conn, cur are database connection and cursor
                #               producer is kafka producer for output
                #               fault_id, asset_id, error_key, closed_on_time, street_id, street_name are for the deleted fault asset
                
                cur.execute("select group_id from fault_groups where asset_id=%s and fault_id=%s and error_key=%s and is_closed='f'", (asset_id, fault_id, error_key))
                group_id_row = cur.fetchone()   
                
                if group_id_row is None:
                        #this fault doesn't exist in a group, we don't need to do any group operations
                        return            
                        
                #this fault asset exists in a group, get the group id 
                group_id = group_id_row[0]
                
                #the list of fault assets in the group except the deleted one
                group_asset_list_except_deleted_one = []
                
                #get fault assets in the group
                cur.execute("select asset_id from fault_groups where group_id=%s and is_closed='f'", (group_id,))
                asset_id_rows = cur.fetchall()
                
                for row in asset_id_rows:
                        if row[0] != asset_id:
                                group_asset_list_except_deleted_one.append(row[0])
                
                                
                #use both local street neighbors and global neighbors to create the neighbor graph                                
                self.graphCreate(conn, cur, street_id, street_name)
                
                #get all open faults with the given error_key
                cur.execute("select id, error_key, is_open, first_reported_on, asset_id, closed_on from faults where error_key=%s and closed_on is null", (error_key,))  
                rows = cur.fetchall()
                
                #fault asset id in the current time
                fault_asset_id_list = []

                for row in rows:
                        fault_asset_id_list.append(row[4])
                
                #fault asset subgraph connected component size detection
                fault_subgraph_nodes = [id for id in fault_asset_id_list if id in self.all_graph_nodes]              
                        
                G_fault = self.G.subgraph(fault_subgraph_nodes)
                
                
                group_asset_set_except_deleted_one = set(group_asset_list_except_deleted_one)
                newly_formed_group_num = 0
                                
                for comp in nx.connected_components(G_fault):
                        #comp is a set of asset id within each connected component
                        comp_size = len(comp)                        
                        set_intersection = group_asset_set_except_deleted_one & comp
                        
                        if len(set_intersection)>0 and comp_size >= self.num_group_faults_limit:
                                #the component, that the new fault asset belongs to, forms a group
                                newly_formed_group_num = newly_formed_group_num + 1
                
                #we have not made any changes to 'fault_groups' table yet
                #need to update 'fault_groups' table here
                if newly_formed_group_num == 0:
                        #the group closed
                        self.groupCloseDeleteFault(conn, cur, producer, mongoCollection, group_id, error_key)
                        
                elif newly_formed_group_num == 1:
                        #the group updated after asset is deleted
                        self.groupUpdateDeleteFault(conn, cur, producer, mongoCollection, group_id, asset_id, error_key)
                        
                else:
                        #newly_formed_group_num > 1
                        #group splitted, more than one group is formed
                        self.groupSplitDeleteFault(conn, cur, producer, mongoCollection, group_id, asset_id, error_key, G_fault, group_asset_set_except_deleted_one)                              
                                                                                

        def sendPartitionFaultsAll(self, iter):
        
                #create connection to the database
                try:
                    #conn = psycopg2.connect("dbname='caba_test' user='wenzhao' password='zmax1987' host='localhost' port='5432'")
                    conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s port=%s" % (self.pg_dbname, self.pg_username, self.pg_password, self.pg_host, self.pg_port))
                    #print "connected!"   
                except psycopg2.Error as e:
                    print "I am unable to connect to the database"
                    print e

                #define cursor
                cur = conn.cursor()
                
                producer = KafkaProducer(bootstrap_servers=['localhost:9092'])    
                
                #MongoDB client
                mongoClient = MongoClient()
                #MongoDB db name
                mongoDBName = mongoClient[self.mongo_dbname]
                #MongoDB collection name
                mongoCollection = mongoDBName[self.mongo_collection_name]
                   
        
                for record in iter:
                
                        try:
                                
                                #connection.send(record)
                        
                                field_list = record.split(",")                        
                                fault_id = int(field_list[0])
                                
                                fault_exists = False
                        
                                #check if this new asset already exists in the faults table
                                cur.execute("select count(*) from faults where id=%s", (fault_id,))
                                count_row = cur.fetchone()
                        
                                if count_row[0] > 0:
                                        #there is already an fault with this fault id, we need to update this fault
                                        #so we first delete it, then insert it
                                        fault_exists = True
                                        
                                        cur.execute("delete from faults where id=%s", (fault_id,))
                                
                                #insert this record to the faults table        
                                file_record = StringIO.StringIO(record)
                                cur.copy_from(file_record, 'faults', sep=',', null='')
                                file_record.close()
                        
                                #commit the update for this fault record
                                conn.commit()
                                
                                
                                #get the important fields from the newly inserted fault record 
                                cur.execute("select id, error_key, is_open, first_reported_on, asset_id, closed_on from faults where id=%s", (fault_id,))
                                fault_row = cur.fetchone()
                                
                                #error_key is a string
                                error_key = fault_row[1]
                                #asset_id is of type integer
                                asset_id = fault_row[4]
                                #closed_on_time is of type datetime.datetime or None
                                closed_on_time = fault_row[5]
                                
                                if fault_exists and closed_on_time is not None:
                                        #this is fault delete event
                                        event_type = 0
                                elif not fault_exists and closed_on_time is None:
                                        #this is new fault add event      
                                        event_type = 1
                                elif fault_exists and closed_on_time is None:
                                        #this is fault update event
                                        event_type = 2          
                                else:
                                        #when fault_exists == False, and closed_on_time is not None
                                        #this is null event, don't need to do anything
                                        event_type = 3       
                                
                                
                                cur.execute("select s.street_id, s.street_name from assets_map_complete as a, streets as s where a.id=%s and lower(a.street_name) = lower(s.street_name)", (asset_id,))
                                street_row = cur.fetchone()
                        
                                if street_row is None:
                                        #the asset doesn't exist, or the street_name of this asset is empty
                                        #print "Error:  Asset %d doesn't exist in our database." % asset_id
                                        #msg = "Error:  Asset %d doesn't exist in our database." % asset_id
                                        msg = "Warning:  Asset %d has empty reverse geo-coded street name in our database." % asset_id
                                        producer.send('test-python-kafka', msg)
                                        continue
                        
                                #get the street id of this fault asset
                                street_id = street_row[0]
        			street_name = street_row[1]
                                
                                                                
                                if event_type == 2 or event_type == 3:
                                        #if event_type == 2 or 3, this is fault update event or null event, no group operation is needed
                                        continue
                                
                                if event_type == 0:
                                        #if event_type == 0, this is fault delete event, possible group operation:
                                        #       group close, group update, group split 
                                        
                                        self.faultDeleteEventProcess(conn, cur, producer, mongoCollection, fault_id, asset_id, error_key, closed_on_time, street_id, street_name)
                                                                                
                                if event_type == 1:
                                        #if event_type == 1, this is new fault add event, possible group operation:
                                        #       group create, group update, group merge
                                        
                                        self.faultAddEventProcess(conn, cur, producer, mongoCollection, fault_id, asset_id, error_key, closed_on_time, street_id, street_name)        
                                
                                                                                                                                                     
                        except Exception as e:
                                print "I am unable to insert fault id %d into faults table." % fault_id
                                print record
        		        print e
                                print traceback.format_exc()
        
                
                #close the connection
                cur.close()
                conn.close()        
    
    
        def run(self, zkQuorum, topic, partitionPeriod):
                # Create a local StreamingContext with two working thread and batch interval of 10 seconds
                sc = SparkContext("local[2]", "SparkFaultsConsumer")
                ssc = StreamingContext(sc, partitionPeriod)
                
                kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, "faults-consumer-group", {topic: 1})

                lines = kafkaStream.map(lambda x: x[1])
        
                #load results to database
                lines.foreachRDD(lambda rdd: rdd.foreachPartition(self.sendPartitionFaultsAll))
        
        
                #extract id, is_open, first_reported_on, asset_id, closed_on from each fault record
                #extracted_lines = lines.map(self.fieldExtract)
        
                #load results to database
                #extracted_lines.foreachRDD(lambda rdd: rdd.foreachPartition(self.sendPartitionFaultsMap))
        

                ssc.start()
                ssc.awaitTermination()
                
                
                
if __name__ == "__main__":
        
        if len(sys.argv) != 3:
                print("Usage: spark_faults.py <zk> <topic>")
                exit(-1)
        
        zkQuorum, topic = sys.argv[1:]
        
        fault_max_ratio = 0.3
        num_group_faults_limit = 3
        street_min_num_assets_to_report = 10
        
        faultStreamProcessor = FaultStreamProcessor(fault_max_ratio, num_group_faults_limit, street_min_num_assets_to_report)
        
        #zkQuorum is zookeeper address, like localhost:2181
        #topic is the fault stream topic
        #partitionPeriod is the batch interval (number of seconds) for the fault stream processing
        partitionPeriod = 10
        
        #start fault stream processor
        faultStreamProcessor.run(zkQuorum, topic, partitionPeriod)
        
        
