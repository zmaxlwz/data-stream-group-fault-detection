import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import googlemaps as gmaps
import psycopg2
import StringIO
import math
import traceback
import json

#in this version, I need to:
#1. for update events, if lat, long values change, I need to do processing too
#2. add dynamicly enlarged region function to the global neighbor generation 


class AssetStreamProcessor:
        def __init__(self):
                self.client = gmaps.Client(key='AIzaSyBdXdfOPuqF-GtCfsjesJNwmYNbJBixk80')
                self.EARTH_CIRCUMFERENCE = 6378137     # earth circumference in meters
                self.radius = 150       # the radius (in meters) for global neighbor generation, this radius defines a rectangle neighborhood region 
                self.latlongEpsilon = 0.000001          #the latitude and longitude epsilon value, if two values differ by >= this value, the two values are different
                
                with open('../config/config.json') as config_file:    
                    config_data = json.load(config_file)
                    
                self.pg_dbname = config_data['pg_dbname']
                self.pg_username = config_data['pg_username']
                self.pg_password = config_data['pg_password']
                self.pg_host = config_data['pg_host']
                self.pg_port = config_data['pg_port']
                #self.mongo_dbname = config_data['mongo_dbname']
                #self.mongo_collection_name = config_data['mongo_collection_name']
                

        """Distance helper function."""

        #compute great circle distance between two points 
        #the input is two tuples, representing two points on earth
        #each tuple has two elements, which are the latitude and longitude of a position point
        def great_circle_distance(self, latlong_a, latlong_b):
                """
                >>> coord_pairs = [
                ...     # between eighth and 31st and eighth and 30th
                ...     [(40.750307,-73.994819), (40.749641,-73.99527)],
                ...     # sanfran to NYC ~2568 miles
                ...     [(37.784750,-122.421180), (40.714585,-74.007202)],
                ...     # about 10 feet apart
                ...     [(40.714732,-74.008091), (40.714753,-74.008074)],
                ...     # inches apart
                ...     [(40.754850,-73.975560), (40.754851,-73.975561)],
                ... ]

                >>> for pair in coord_pairs:
                ...     great_circle_distance(pair[0], pair[1]) # doctest: +ELLIPSIS
                83.325362855055...
                4133342.6554530...
                2.7426970360283...
                0.1396525521278...
                """
                lat1, lon1 = latlong_a
                lat2, lon2 = latlong_b

                dLat = math.radians(lat2 - lat1)
                dLon = math.radians(lon2 - lon1)
                a = (math.sin(dLat / 2) * math.sin(dLat / 2) +
                    math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
                    math.sin(dLon / 2) * math.sin(dLon / 2))
                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                d = self.EARTH_CIRCUMFERENCE * c

                return d



        def fieldExtract(self, line):
                #line is comma delimited, we need to split it and get field 0, 3, 4, 9, 10, 12, 15, 
                #which are id, lat, long, install_date, commission_date, is_deleted, last_modified_on
                #then combine them into a string
                record_list = line.split(",")
                index = [0,3,4,9,10,12,15]
                extracted_list = [record_list[i] for i in index]
                result_line = ','.join(extracted_list)
                return result_line

        def geoReverse(self, line):
                #each input line is "id,lat,long"
                #need to split it and georeverse according to Google's API
                field_list = line.split(",")
                id = field_list[0]
                latitude = float(field_list[1])
                longitude = float(field_list[2])
                reverse_addr=self.client.reverse_geocode((latitude, longitude))
        
                
                try:
                        #the returned address is not empty                             
                        address_component = reverse_addr[0]['address_components']
                        #append street name
                        street_name = [obj['long_name'] for obj in address_component if obj['types'][0] == 'route']        
                        field_list.append(street_name[0])
                        #append city name
                        #city_name = [obj['long_name'] for obj in address_component if obj['types'][0] == 'administrative_area_level_1']
                        city_name = [obj['long_name'] for obj in address_component if obj['types'][0] == 'locality']
                        field_list.append(city_name[0])
                        #append country name
                        country_name = [obj['long_name'] for obj in address_component if obj['types'][0] == 'country']
                        field_list.append(country_name[0])
        
                except IndexError as indErr:
                        print 'IndexError occurred in geocoding, list may be empty: ', indErr
                        field_list = field_list[:7]
                        field_list.append("")       
                        field_list.append("")       
                        field_list.append("")  
                
                except KeyError as keyErr:
                        print 'KeyError occurred in geocoding, object does not have the key: ', keyErr
                        field_list = field_list[:7]
                        field_list.append("")       
                        field_list.append("")       
                        field_list.append("")        
                
                #combine the names together
                result_line = ','.join(field_list)
                return result_line
                
                
        def generateStreetLocalNeighbors(self, conn, cur, field_list):
                #conn:  this is the database connection
                #cur:   this is the database cursor
                #field_list:  this is the field list from a record
                #           [id, latitude, longitude, installation_date, commissioning_date, is_deleted, last_modified_on, street_name, city_name, country_name]
                
                
                asset_id = int(field_list[0])
                
                #if the street name is empty string, then don't do street name insertion or neighbor generation
                #field_list[7] is the street name, may have capital letters
                if not field_list[7]:
                        #street_name is empty string 
                        #continue
                        return
        
        
                #********** insert the street of the asset into streets table ***************
        
                #check if the street name exists in 'streets' table 
                #cur.execute("select count(*) from assets_map_complete as a, streets as s where a.id=%s and lower(a.street_name) = lower(s.street_name)", (asset_id,))
                cur.execute("select count(*) from streets where street_name=%s", (field_list[7].lower(),))
                count_row = cur.fetchone()
        
                #check if need to insert the street into the 'streets' table
                #assumption: (1)the 'streets' table already exists, (2) the street_id is a serial type
                #if this is asset delete event, the street must have already existed, it will not add street here
                #only asset add event can add street here
                if count_row[0] == 0:
                        #the street name doesn't exist in the 'streets' table, insert the street into the 'streets' table                                
                        #cur.execute("insert into streets (street_id, street_name) values (nextval('street_seq'), %s)", (field_list[7],))
                        cur.execute("insert into streets (street_name) values (%s)", (field_list[7].lower(),))
                
                
                #********** regenerate neighbors for the street ***************  
        
                #get the street id of this asset
                #cur.execute("select s.street_id, lower(s.street_name) from assets_map_complete as a, streets as s where a.id=%s and lower(a.street_name) = lower(s.street_name)", (asset_id,))
                cur.execute("select street_id, lower(street_name) from streets where street_name=%s", (field_list[7].lower(),))
                street_row = cur.fetchone()
                #street id   
                street_id = street_row[0]
                #street name, in lowercase
                street_name = street_row[1]
        
                #delete the neighbors records for this street in the asset_neighbors table
                cur.execute("delete from asset_neighbors where street_id=%s", (street_id,))
        
        
                #second commit: if the street has less than 3 assets, it will skip neighbor generation, so it is better to do commit here
                #commit the update for this asset record into the database
                conn.commit()
        
        
                #get all assets record in this street from the 'assets_map_complete' table
                cur.execute("select id, latitude, longitude from assets_map_complete where is_deleted = 'f' and lower(street_name)=%s", (street_name,))
        
                #all assets rows in the street
                asset_rows_in_one_street = cur.fetchall()
        
                num_assets_in_the_street = len(asset_rows_in_one_street)        

                #if the number of assets in the street is less than 3, we will skip neighbor generation
                if num_assets_in_the_street < 3:
                        #continue
                        return street_id

                #print "there are", num_assets_in_the_street, "assets in the street."

                asset_id_list = []
                asset_latitude_list = []
                asset_longitude_list = []

                #get all asset id, latitude, longitude in one street
                for asset_row in asset_rows_in_one_street:
                        asset_id_list.append(asset_row[0])
                        asset_latitude_list.append(asset_row[1])
                        asset_longitude_list.append(asset_row[2])

                #for each asset, compute the closest neighbor and second closest neighbor
                for target_asset_index in range(num_assets_in_the_street):
                        #target asset info
                        target_asset_id = asset_id_list[target_asset_index]
                        target_asset_latitude = asset_latitude_list[target_asset_index]
                        target_asset_longitude = asset_longitude_list[target_asset_index]
                        #the distance to closest neighbor and second closest neighbor
                        closest_neighbor_distance = sys.maxint
                        second_closest_neighbor_distance = sys.maxint
        
                        #compute the target asset distance to other assets in the street
                        for compare_asset_index in range(num_assets_in_the_street):
                                if compare_asset_index == target_asset_index:
                                        continue
                                compare_asset_id = asset_id_list[compare_asset_index]
                                compare_asset_latitude = asset_latitude_list[compare_asset_index]
                                compare_asset_longitude = asset_longitude_list[compare_asset_index]       
                
                                #compute distance from target_asset to compare_asset
                                target_lat_long = (target_asset_latitude, target_asset_longitude)
                                compare_lat_long = (compare_asset_latitude, compare_asset_longitude)
                                distance_in_meters = self.great_circle_distance(target_lat_long, compare_lat_long)
                
                                #if the distance is smaller than 2 meters, then skip it, they are too near each other, maybe they are just the same asset
                                if distance_in_meters < 2:
                                        continue
                
                                if distance_in_meters < closest_neighbor_distance:
                                        #this distance is smaller than the closest neighbor distance
                                        if closest_neighbor_distance < sys.maxint:
                                                second_closest_neighbor_distance = closest_neighbor_distance
                                                second_closest_neighbor_id = closest_neighbor_id
                                        closest_neighbor_distance = distance_in_meters
                                        closest_neighbor_id = compare_asset_id
                                elif distance_in_meters < second_closest_neighbor_distance:
                                        second_closest_neighbor_distance = distance_in_meters
                                        second_closest_neighbor_id = compare_asset_id
                
                        #if the closest distance or second closest distance is still sys.maxint, skip, don't insert into database
                        if closest_neighbor_distance == sys.maxint or second_closest_neighbor_distance == sys.maxint:
                                print "invalid distance occurs, skip the record, don't insert into the asset_neighbors table"
                                continue
                        
                        #already get the closest neighbor and second closest neighbor 
                        #store the two neighbors into the database table 'asset_neighbors', attributes include: 
                        #street_id, target_asset_id, target_asset_latitude, target_asset_longitude, closest_neighbor_id, closest_neighbor_distance, second_closest_neighbor_id, second_closest_neighbor_distance
        
                        #clear asset_neighbors table
                        try:               
                                #insert neighbors of the target asset into asset_neighbors table
                                cur.execute("insert into asset_neighbors(street_id, asset_id, latitude, longitude, first_neighbor_id, distance_to_first_neighbor, second_neighbor_id, distance_to_second_neighbor) values (%s, %s, %s, %s, %s, %s, %s, %s)", 
                                                (street_id, target_asset_id, target_asset_latitude, target_asset_longitude, closest_neighbor_id, closest_neighbor_distance, second_closest_neighbor_id, second_closest_neighbor_distance))
                        except:
                                print "I am unable to insert into asset_neighbors table for street %d, asset %d." % (street_id, target_asset_id)
        
        
                #*********** finished generating neighbors for assets in the street *********
        
                #final commit:
                #commit the update for this asset record into the database
                conn.commit()    
                                
                return street_id

        
        #compute the longitude of points with the same latitude and distance away from the input point 
        #latlong_a is a tuple, representing the latitude and longitude of the input point   
        #distance is in meters 
        #the returned value is a tuple, including two longitude values, represent the left and right point longitude value     
        def same_lat_get_long(self, latlong_a, distance):
                lat1, lon1 = latlong_a
                lat2 = lat1
                                
                #perform the reverse process of great_circle_distance
                c = distance*1.0 / self.EARTH_CIRCUMFERENCE      
                b = math.tan(c / 2)                
                a = (b*b)/(1+b*b)  
                
                #dLon is in radians
                dLon = 2 * math.asin(math.sqrt(a/(math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)))))   
                
                lon2Small = lon1 - math.degrees(dLon)
                lon2Large = lon1 + math.degrees(dLon)
                
                return (lon2Small, lon2Large)
        
                
        #compute the latitude of points with the same longitude and distance away from the input point 
        #latlong_a is a tuple, representing the latitude and longitude of the input point   
        #distance is in meters 
        #the returned value is a tuple, including two latitude values, represent the up and down point latitude value             
        def same_long_get_lat(self, latlong_a, distance):
                lat1, lon1 = latlong_a
                lon2 = lon1
                
                #perform the reverse process of great_circle_distance
                c = distance*1.0 / self.EARTH_CIRCUMFERENCE      
                b = math.tan(c / 2)                
                a = (b*b)/(1+b*b)  
                
                #dLat is in radians
                dLat = 2 * math.asin(math.sqrt(a))  
                
                lat2Small = lat1 - math.degrees(dLat)
                lat2Large = lat1 + math.degrees(dLat)
                
                return (lat2Small, lat2Large)
                
        def getAssetRowsInRegion(self, conn, cur, latlong, radius):
                #conn:  this is the database connection
                #cur:   this is the database cursor
                #latlong: this is a tuple, like (lat, long)
                #radius:  this is the radius for the rectangle region
                                
                #compute the lat range for the neighborhood region, self.radius is defined in the __init__ method
                (latRangeLow, latRangeHigh) = self.same_long_get_lat(latlong, radius)
                #compute the long range for the neighborhood region, self.radius is defined in the __init__ method
                (lonRangeLow, lonRangeHigh) = self.same_lat_get_long(latlong, radius)
                
                #get all assets within this neighborhood region, including this newly added asset
                cur.execute("select id, latitude, longitude from assets_map_complete where is_deleted = 'f' and latitude between %s and %s and longitude between %s and %s", (latRangeLow, latRangeHigh, lonRangeLow, lonRangeHigh))
        
                #all assets rows in the neighborhood region
                #if there is no assets in the region, asset_rows_in_neighborhood_region will be [], an empty list
                asset_rows_in_neighborhood_region = cur.fetchall()
                
                #the returned value is a list of tuples, or empty list
                return asset_rows_in_neighborhood_region
                       
        
        def addGlobalNeighbor(self, conn, cur, field_list, street_id):
                #conn:  this is the database connection
                #cur:   this is the database cursor
                #field_list:  this is the field list from a record
                #           [id, latitude, longitude, installation_date, commissioning_date, is_deleted, last_modified_on, street_name, city_name, country_name]
                
                #the input asset id
                asset_id = int(field_list[0])
                
                #get the lat, long for the input asset
                latitude = float(field_list[1])
                longitude = float(field_list[2])
                
                #construct the lat, long tuple 
                latlong = (latitude, longitude)
                
                #get all assets rows in the neighborhood region                                
                asset_rows_in_neighborhood_region = self.getAssetRowsInRegion(conn, cur, latlong, self.radius)
        
                num_assets_in_neighborhood_region = len(asset_rows_in_neighborhood_region)        

                #if the number of assets in the neighborhood region is less than 3, we will try a larger region
                if num_assets_in_neighborhood_region < 3:
                                                
                        #get all assets rows in a larger neighborhood region                                
                        asset_rows_in_neighborhood_region = self.getAssetRowsInRegion(conn, cur, latlong, 2*self.radius)
        
                        num_assets_in_neighborhood_region = len(asset_rows_in_neighborhood_region)        
                        
                        
                #if the number of assets in the larger neighborhood region is still less than 3, we will skip global neighbor update
                if num_assets_in_neighborhood_region < 3:
                        #continue                        
                        return
                        
                
                #within the neighborhood, we do two things:
                #       1) find the two closest neighbors for the inserted asset
                #       2) for each of the other assets(not the inserted one), check if the inserted asset can be one of the two closest neighbor
                
                asset_id_list = []
                asset_latitude_list = []
                asset_longitude_list = []

                #get all asset id, latitude, longitude in one street
                for asset_row in asset_rows_in_neighborhood_region:
                        asset_id_list.append(asset_row[0])
                        asset_latitude_list.append(asset_row[1])
                        asset_longitude_list.append(asset_row[2])
                
                
                #set the target asset as the input asset
                target_asset_id = asset_id
                target_asset_latitude = latitude
                target_asset_longitude = longitude
                #the distance to closest neighbor and second closest neighbor
                closest_neighbor_distance = sys.maxint
                second_closest_neighbor_distance = sys.maxint

                #compute the target asset distance to other assets in the street
                for compare_asset_index in range(num_assets_in_neighborhood_region):
                        compare_asset_id = asset_id_list[compare_asset_index]
                        compare_asset_latitude = asset_latitude_list[compare_asset_index]
                        compare_asset_longitude = asset_longitude_list[compare_asset_index]       
                        
                        if compare_asset_id == target_asset_id:
                                #this is the target asset 
                                continue
                        
                        #compute distance from target_asset to compare_asset
                        target_lat_long = (target_asset_latitude, target_asset_longitude)
                        compare_lat_long = (compare_asset_latitude, compare_asset_longitude)
                        distance_in_meters = self.great_circle_distance(target_lat_long, compare_lat_long)
        
                        #if the distance is smaller than 2 meters, then skip it, they are too near each other, maybe they are just the same asset
                        if distance_in_meters < 2:
                                continue
                        
                        #check if can update the target asset's closest and second closest neighbor        
                        if distance_in_meters < closest_neighbor_distance:
                                #this distance is smaller than the closest neighbor distance
                                if closest_neighbor_distance < sys.maxint:
                                        second_closest_neighbor_distance = closest_neighbor_distance
                                        second_closest_neighbor_id = closest_neighbor_id
                                closest_neighbor_distance = distance_in_meters
                                closest_neighbor_id = compare_asset_id
                        elif distance_in_meters < second_closest_neighbor_distance:
                                second_closest_neighbor_distance = distance_in_meters
                                second_closest_neighbor_id = compare_asset_id
                                
                        #check if the target asset can be the closest or second closest neighbor of the compare asset
                                                
                        #get the closest and second closest neighbor distance to the compare asset
                        cur.execute("select asset_id, first_neighbor_id, distance_to_first_neighbor, second_neighbor_id, distance_to_second_neighbor from global_asset_neighbors where asset_id=%s", (compare_asset_id,))
                        compare_asset_row = cur.fetchone()
                        
                        if compare_asset_row is None:
                                #there is no record for compare_asset in the global_asset_neighbors table
                                continue
                        
                        compare_asset_closest_neighbor_id = compare_asset_row[1]
                        compare_asset_closest_neighbor_distance = compare_asset_row[2]
                        compare_asset_second_closest_neighbor_id = compare_asset_row[3]
                        compare_asset_second_closest_neighbor_distance = compare_asset_row[4]
                        
                        if distance_in_meters < compare_asset_closest_neighbor_distance:
                                #set closest and second closest neighbor and distance
                                cur.execute("update global_asset_neighbors set first_neighbor_id=%s, distance_to_first_neighbor=%s, second_neighbor_id=%s, distance_to_second_neighbor=%s where asset_id=%s", (target_asset_id, distance_in_meters, compare_asset_closest_neighbor_id, compare_asset_closest_neighbor_distance, compare_asset_id))
                        elif distance_in_meters < compare_asset_second_closest_neighbor_distance:
                                #set second closest neighbor and distance
                                cur.execute("update global_asset_neighbors set second_neighbor_id=%s, distance_to_second_neighbor=%s where asset_id=%s", (target_asset_id, distance_in_meters, compare_asset_id))        
                
                
                #commit the global neighbor update into the database        
                conn.commit()
                                
                #if the closest distance or second closest distance is still sys.maxint, skip, don't insert into database
                if closest_neighbor_distance == sys.maxint or second_closest_neighbor_distance == sys.maxint:
                        print "invalid distance occurs in addGlobalNeighbor, skip the record, don't insert into the global_asset_neighbors table"
                        return
                
                #already get the closest neighbor and second closest neighbor 
                #store the two neighbors into the database table 'global_asset_neighbors', attributes include: 
                #street_id, target_asset_id, target_asset_latitude, target_asset_longitude, closest_neighbor_id, closest_neighbor_distance, second_closest_neighbor_id, second_closest_neighbor_distance

                #insert into global_asset_neighbors table
                try:               
                        #insert neighbors of the target asset into global_asset_neighbors table
                        cur.execute("insert into global_asset_neighbors(street_id, asset_id, latitude, longitude, first_neighbor_id, distance_to_first_neighbor, second_neighbor_id, distance_to_second_neighbor) values (%s, %s, %s, %s, %s, %s, %s, %s)", 
                                        (street_id, target_asset_id, target_asset_latitude, target_asset_longitude, closest_neighbor_id, closest_neighbor_distance, second_closest_neighbor_id, second_closest_neighbor_distance))
                except:
                        print "I am unable to insert into global_asset_neighbors table for street %d, asset %d." % (street_id, target_asset_id)

                
                #commit the global neighbor update into the database        
                conn.commit()
                
                
                
        def deleteGlobalNeighbor(self, conn, cur, field_list):
                #conn:  this is the database connection
                #cur:   this is the database cursor
                #field_list:  this is the field list from a record
                #           [id, latitude, longitude, installation_date, commissioning_date, is_deleted, last_modified_on, street_name, city_name, country_name]
                
                #the input asset id
                target_asset_id = int(field_list[0])
                
                #get the lat, long for the input asset
                target_asset_latitude = float(field_list[1])
                target_asset_longitude = float(field_list[2])
                
                #delete this asset record from the global_asset_neighbors table
                cur.execute("delete from global_asset_neighbors where asset_id=%s", (target_asset_id,))
                
                conn.commit()    
                
                #update the closest or second closest neighbor of the other points within the neighborhood region, if it is the target asset
                
                #construct the lat, long tuple 
                latlong = (target_asset_latitude, target_asset_longitude)
                
                #get all assets rows in the neighborhood region, if there is no asset in the region, asset_rows_in_neighborhood_region will be [], empty list      
                asset_rows_in_neighborhood_region = self.getAssetRowsInRegion(conn, cur, latlong, self.radius)
        
                num_assets_in_neighborhood_region = len(asset_rows_in_neighborhood_region)        

                #within the neighborhood, we do two things:
                #       1) check if the target asset is closest or second closest neighbor to any of the assets within the neighborhood
                #       2) if the target asset is, need to find the new two closest neighbors for these assets within the neighborhood
                
                asset_id_list = []
                asset_latitude_list = []
                asset_longitude_list = []

                #get all asset id, latitude, longitude in one street
                for asset_row in asset_rows_in_neighborhood_region:
                        asset_id_list.append(asset_row[0])
                        asset_latitude_list.append(asset_row[1])
                        asset_longitude_list.append(asset_row[2])
                
                #compute the target asset distance to other assets in the street
                for compare_asset_index in range(num_assets_in_neighborhood_region):
                        compare_asset_id = asset_id_list[compare_asset_index]
                        compare_asset_latitude = asset_latitude_list[compare_asset_index]
                        compare_asset_longitude = asset_longitude_list[compare_asset_index]       
                        
                        if compare_asset_id == target_asset_id:
                                #this is the target asset 
                                continue
                                
                        #get the closest and second closest neighbor distance to the compare asset
                        cur.execute("select asset_id, first_neighbor_id, distance_to_first_neighbor, second_neighbor_id, distance_to_second_neighbor from global_asset_neighbors where asset_id=%s", (compare_asset_id,))
                        compare_asset_row = cur.fetchone()
                        
                        compare_asset_closest_neighbor_id = compare_asset_row[1]
                        compare_asset_closest_neighbor_distance = compare_asset_row[2]
                        compare_asset_second_closest_neighbor_id = compare_asset_row[3]
                        compare_asset_second_closest_neighbor_distance = compare_asset_row[4]
                        
                        if compare_asset_closest_neighbor_id == target_asset_id or compare_asset_second_closest_neighbor_id == target_asset_id:
                                #need to update the two closest neighbor for the compare asset
                                
                                #the distance to closest neighbor and second closest neighbor
                                closest_neighbor_distance = sys.maxint
                                second_closest_neighbor_distance = sys.maxint

                                #compute the target asset distance to other assets in the street
                                for search_asset_index in range(num_assets_in_neighborhood_region):
                                        search_asset_id = asset_id_list[search_asset_index]
                                        search_asset_latitude = asset_latitude_list[search_asset_index]
                                        search_asset_longitude = asset_longitude_list[search_asset_index]       
                        
                                        if search_asset_id == target_asset_id or search_asset_id == compare_asset_id:
                                                #this is the target asset or compare asset
                                                continue
                        
                                        #compute distance from search_asset to compare_asset
                                        search_lat_long = (search_asset_latitude, search_asset_longitude)
                                        compare_lat_long = (compare_asset_latitude, compare_asset_longitude)
                                        distance_in_meters = self.great_circle_distance(search_lat_long, compare_lat_long)
        
                                        #if the distance is smaller than 2 meters, then skip it, they are too near each other, maybe they are just the same asset
                                        if distance_in_meters < 2:
                                                continue
                        
                                        #check if can update the target asset's closest and second closest neighbor        
                                        if distance_in_meters < closest_neighbor_distance:
                                                #this distance is smaller than the closest neighbor distance
                                                if closest_neighbor_distance < sys.maxint:
                                                        second_closest_neighbor_distance = closest_neighbor_distance
                                                        second_closest_neighbor_id = closest_neighbor_id
                                                closest_neighbor_distance = distance_in_meters
                                                closest_neighbor_id = search_asset_id
                                        elif distance_in_meters < second_closest_neighbor_distance:
                                                second_closest_neighbor_distance = distance_in_meters
                                                second_closest_neighbor_id = search_asset_id
                                     
                                #if the closest distance or second closest distance is still sys.maxint, skip, don't insert into database
                                if closest_neighbor_distance == sys.maxint or second_closest_neighbor_distance == sys.maxint:
                                        print "invalid distance occurs in deleteGlobalNeighbor, skip the record, delete this asset from the global_asset_neighbors table"
                                        #cur.execute("delete from global_asset_neighbors where asset_id=%s", (compare_asset_id,))
                                        continue
                                        
                                #update this compare asset neighbors in global_asset_neighbors table
                                try:               
                                        cur.execute("update global_asset_neighbors set first_neighbor_id=%s, distance_to_first_neighbor=%s, second_neighbor_id=%s, distance_to_second_neighbor=%s where asset_id=%s", (closest_neighbor_id, closest_neighbor_distance, second_closest_neighbor_id, second_closest_neighbor_distance, compare_asset_id))
                                except:
                                        print "I am unable to update global_asset_neighbors table for asset %d." % (compare_asset_id,)
                                        
                
                #commit all updates for compare assets                
                conn.commit()                    
                
                      
                        
        def sendPartitionAssetsMap(self, iter):
                #this function loads assets map records into the assets_map_complete table, which includes all mapped attributes
        
                #create connection to the database
                try:
                    #conn = psycopg2.connect("dbname='fault_test' user='wenzhao' password='zmax1987' host='localhost' port='5432'")
                    conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s port=%s" % (self.pg_dbname, self.pg_username, self.pg_password, self.pg_host, self.pg_port))
                    #print "connected!"   
                except psycopg2.Error as e:
                    print "I am unable to connect to the database"
                    print e

                #define cursor
                cur = conn.cursor()
               
                
                for record in iter:
                
                        try:
                                #connection.send(record)
                                field_list = record.split(",")
                        
                                asset_id = int(field_list[0])
                        
                                #********** insert the asset record into assets_map_complete table ***************
                                
                                asset_exists = False
                        
                                #check if this new asset already exists in the assets_map_complete table
                                cur.execute("select count(*) from assets_map_complete where id=%s", (asset_id,))
                                count_row = cur.fetchone()
                        
                                if count_row[0] > 0:
                                        #there is already an asset with this asset id, we need to update this asset
                                        #so we first delete it, then insert it
                                        asset_exists = True
                                        
                                        #get the existing record for this asset
                                        cur.execute("select * from assets_map_complete where id=%s", (asset_id,))
                                        #existing_record_field_list is a tuple
                                        existing_record_field_list = cur.fetchone()
                                                                                
                                        #delete the existing record for this asset from the assets_map_complete table
                                        cur.execute("delete from assets_map_complete where id=%s", (asset_id,))
                        
        			if not field_list[3]:
        				field_list[3] = None
        			if not field_list[4]:
        				field_list[4] = None
        			if not field_list[5]:
        				field_list[5] = 'false'
        			if not field_list[6]:
        				field_list[6] = None
						
                                #insert record into assets_map_complete table
                                cur.execute("insert into assets_map_complete (id, latitude, longitude, installation_date, commissioning_date, is_deleted, last_modified_on, street_name, city_name, country_name) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                                                (int(field_list[0]), float(field_list[1]), float(field_list[2]), field_list[3], field_list[4], field_list[5], field_list[6], field_list[7], field_list[8], field_list[9]))
                        
                                #First commit, the following updates may get skipped, so it is better to do commit here
                                #commit the update for this asset record into the database
                                conn.commit()
                                
                                #query for this newly added asset, and check if 'is_deleted' is True
                                cur.execute("select id, is_deleted from assets_map_complete where id=%s", (asset_id,))
                                asset_row = cur.fetchone()
                                
                                #is_deleted is a boolean variable
                                is_deleted = asset_row[1]
                                
                                #determine if the asset event is delete(0), add(1), or update(2). Update event doesn't influence neighbors
                                if asset_exists and is_deleted:
                                        #this is delete event
                                        event_type = 0
                                elif not asset_exists and not is_deleted:
                                        #this is add event 
                                        event_type = 1
                                elif asset_exists and not is_deleted:
                                        #this is update event
                                        event_type = 2
                                else:
                                        #asset_exists == False, and is_deleted == True
                                        #this is null event, don't need to do anything
                                        event_type = 3        
                                                                        
                                #check the event type                         
                                if event_type == 0 or event_type == 1:
                                        #this is asset delete or asset add event, we need to update the street local neighbors                                        
                                        #generate street local neighbors for this asset
                                        street_id = self.generateStreetLocalNeighbors(conn, cur, field_list)   
                                
                                        #need to add global neighbor
                                        if event_type == 1 and street_id is not None:
                                                #this is an asset add event, need to add this asset to the global neighbor table, and update its neighbor region
                                                self.addGlobalNeighbor(conn, cur, field_list, street_id)
                                
                                        #need to delete global neighbor
                                        if event_type == 0:
                                                #this is an asset delete event, need to delete the asset from global neighbor table, and update its neighbor region
                                                self.deleteGlobalNeighbor(conn, cur, field_list)  
                                                
                                elif event_type == 2:
                                        #this is asset update event
                                        #1. check if lat, long value changes
                                        #2. check if street name changes
                                        #3. update street local neighbors
                                        #4. update global neighbors
                                        
                                        if abs(existing_record_field_list[1] - float(field_list[1])) >= self.latlongEpsilon or abs(existing_record_field_list[2] - float(field_list[2])) >= self.latlongEpsilon:
                                                #the update changes the lat, long value for this asset
                                                
                                                #the original and current street name for this asset
                                                original_street_name = existing_record_field_list[7].lower()
                                                current_street_name = field_list[7].lower()
                                                
                                                #update street local neighbors
                                                if original_street_name == current_street_name:
                                                        #after the lat, long values change, the street name doesn't change
                                                        #only need to regenerate local neighbors on the same street
                                                        street_id = self.generateStreetLocalNeighbors(conn, cur, field_list)   
                                                else:
                                                        #after the lat, long values change, the street name changes
                                                        
                                                        #regenerate local neighbors for the original street
                                                        old_street_id = self.generateStreetLocalNeighbors(conn, cur, existing_record_field_list) 
                                                        #regenerate local neighbors for the new street  
                                                        street_id = self.generateStreetLocalNeighbors(conn, cur, field_list)   
                                                        
                                                #update global neighbors
                                                #1. delete global neighbors for the original asset
                                                self.deleteGlobalNeighbor(conn, cur, existing_record_field_list)  
                                                #2. add back global neighbors for the new asset
                                                if street_id is not None:
                                                        self.addGlobalNeighbor(conn, cur, field_list, street_id)
                                                                                                               
                                        #if the lat, long values don't change, we don't need to do anything for update event                  
                           
                                        
                        except Exception as e:
                                print "There are some errors when processing asset %d map records in sendPartitionAssetsMap function." % asset_id
                                print record
        		        print e
                                print traceback.format_exc()
                
        
                #close the connection
                cur.close()
                conn.close()        
    

        def sendPartitionAssetsAll(self, iter):
                #this function loads assets records into the assets table, which includes all attributes
        
                #create connection to the database
                try:
                    conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s port=%s" % (self.pg_dbname, self.pg_username, self.pg_password, self.pg_host, self.pg_port))
                    #print "connected!"   
                except psycopg2.Error as e:
                    print "I am unable to connect to the database"
                    print e

                #define cursor
                cur = conn.cursor()
        
        
                
                for record in iter:
                
                        try:
                                #connection.send(record)
                                                
                                field_list = record.split(",")                        
                                asset_id = int(field_list[0])
                        
                                #check if this new asset already exists in the assets table
                                cur.execute("select count(*) from assets where id=%s", (asset_id,))
                                count_row = cur.fetchone()
                        
                                if count_row[0] > 0:
                                        #there is already an asset with this asset id, we need to update this asset
                                        #so we first delete it, then insert it
                                        cur.execute("delete from assets where id=%s", (asset_id,))
                        
                                #insert this asset into the assets table
                                file_record = StringIO.StringIO(record)
                                cur.copy_from(file_record, 'assets', sep=',', null='')
                                file_record.close()
                        
                                #commit the update for this asset record into the database
                                conn.commit()
                                                     
                        except Exception as e:
                                print "I am unable to insert asset %d into assets table." % asset_id
                                print record
        		        print e
                                print traceback.format_exc()
        
        
                #close the connection
                cur.close()
                conn.close()  
                
                
        def run(self, zkQuorum, topic, partitionPeriod):
                # Create a local StreamingContext with two working thread and batch interval of 10 seconds
                sc = SparkContext("local[2]", "SparkAssetsConsumer")
                ssc = StreamingContext(sc, partitionPeriod)
                
                kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, "assets-consumer-group", {topic: 1})
        
                #get each line
                lines = kafkaStream.map(lambda x: x[1])
        
                #load results to database
                lines.foreachRDD(lambda rdd: rdd.foreachPartition(self.sendPartitionAssetsAll))
        
        
                #extract id, lat, long, and some other fields 
                extracted_lines = lines.map(self.fieldExtract)
                #append geo reverse street name, city name, country name
                append_lines = extracted_lines.map(self.geoReverse)
                #output to files
                #append_lines.saveAsTextFiles('asset-map')

                #print 'Event recieved in window: ', append_lines.pprint()
        
                #load results to database
                append_lines.foreachRDD(lambda rdd: rdd.foreachPartition(self.sendPartitionAssetsMap))

                ssc.start()
                ssc.awaitTermination()
                          
    


if __name__ == "__main__":
                
        if len(sys.argv) != 3:
                print("Usage: spark_assets.py <zk> <topic>")
                exit(-1)
                
        
        zkQuorum, topic = sys.argv[1:]

        assetStreamProcessor = AssetStreamProcessor()
        
        #zkQuorum is zookeeper address, like localhost:2181
        #topic is the fault stream topic
        #partitionPeriod is the batch interval (number of seconds) for the fault stream processing
        partitionPeriod = 10
        
        #start fault stream processor
        assetStreamProcessor.run(zkQuorum, topic, partitionPeriod)
        
                
        
