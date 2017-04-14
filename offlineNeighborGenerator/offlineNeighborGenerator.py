import psycopg2
from datetime import datetime
import math
import sys
import StringIO
import traceback

#in this class, I will generate streets, local neighbors and global neighbors in an offline fashion

class OfflineNeighborGenerator:
        def __init__(self):
                #self.client = gmaps.Client(key='AIzaSyBdXdfOPuqF-GtCfsjesJNwmYNbJBixk80')
                self.EARTH_CIRCUMFERENCE = 6378137     # earth circumference in meters
                self.radius = 150       # the radius (in meters) for global neighbor generation, this radius defines a rectangle neighborhood region 
                self.latlongEpsilon = 0.000001          #the latitude and longitude epsilon value, if two values differ by >= this value, the two values are different
                
                #create connection to the database
                try:
                    self.conn = psycopg2.connect("dbname='caba_test' user='wenzhao' password='zmax1987' host='localhost' port='5432'")
                    #print "connected!"   
                except psycopg2.Error as e:
                    print "I am unable to connect to the database"
                    print e

                #define cursor
                self.cur = self.conn.cursor()
                
        
        
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
                

        def populateStreetsTable(self):
                #this method populates the streets table using assets_map_complete table
                
                #clear records in the streets table
                self.cur.execute("delete from streets")
                
                #insert street name into streets table, street_id will be automatically generated
                self.cur.execute("insert into streets(street_name) select distinct lower(street_name) from assets_map_complete where street_name <> ''")
                
                #commit the change
                self.conn.commit()
                
                
        def generateStreetLocalNeighbors(self):
                #this method generates street local neighbors
                
                self.cur.execute("select street_id, street_name from streets")
                street_rows = self.cur.fetchall()
                
                num_streets = len(street_rows)
                
                street_id_list = []
                street_name_list = []
                for row in street_rows:
                        street_id_list.append(row[0])
                        street_name_list.append(row[1])
                
                                
                for street_index in range(num_streets):
                        #process each street
                        
                        print "process street ", street_index
                        
                        street_id = street_id_list[street_index]
                        street_name = street_name_list[street_index]
                        
                        #get all assets record in this street from the 'assets_map_complete' table
                        self.cur.execute("select id, latitude, longitude from assets_map_complete where is_deleted = 'f' and lower(street_name)=%s", (street_name,))
        
                        #all assets rows in the street
                        asset_rows_in_one_street = self.cur.fetchall()
        
                        num_assets_in_the_street = len(asset_rows_in_one_street)        

                        #if the number of assets in the street is less than 3, we will skip neighbor generation
                        if num_assets_in_the_street < 3:
                                continue
                                #return street_id

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
                                        self.cur.execute("insert into asset_neighbors(street_id, asset_id, latitude, longitude, first_neighbor_id, distance_to_first_neighbor, second_neighbor_id, distance_to_second_neighbor) values (%s, %s, %s, %s, %s, %s, %s, %s)", 
                                                        (street_id, target_asset_id, target_asset_latitude, target_asset_longitude, closest_neighbor_id, closest_neighbor_distance, second_closest_neighbor_id, second_closest_neighbor_distance))
                                except:
                                        print "I am unable to insert into asset_neighbors table for street %d, asset %d." % (street_id, target_asset_id)
        
        
                        #*********** finished generating neighbors for assets in the street *********
        
                        #final commit:
                        #commit the update for the asset record in the street into the database
                        self.conn.commit()
        
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
                
                
        def getAssetRowsInRegion(self, latlong, radius):
                #conn:  this is the database connection
                #cur:   this is the database cursor
                #latlong: this is a tuple, like (lat, long)
                #radius:  this is the radius for the rectangle region
                                
                #compute the lat range for the neighborhood region, self.radius is defined in the __init__ method
                (latRangeLow, latRangeHigh) = self.same_long_get_lat(latlong, radius)
                #compute the long range for the neighborhood region, self.radius is defined in the __init__ method
                (lonRangeLow, lonRangeHigh) = self.same_lat_get_long(latlong, radius)
                
                #get all assets within this neighborhood region, including this newly added asset
                self.cur.execute("select id, latitude, longitude from assets_map_complete where is_deleted = 'f' and latitude between %s and %s and longitude between %s and %s", (latRangeLow, latRangeHigh, lonRangeLow, lonRangeHigh))
        
                #all assets rows in the neighborhood region
                #if there is no assets in the region, asset_rows_in_neighborhood_region will be [], an empty list
                asset_rows_in_neighborhood_region = self.cur.fetchall()
                
                #the returned value is a list of tuples, or empty list
                return asset_rows_in_neighborhood_region
                       
        
        def generateGlobalNeighbors(self):
                #this method generates global neighbors
                
                self.cur.execute("select id, latitude, longitude, street_id from assets_map_complete as a, streets as s where a.is_deleted = 'f' and lower(a.street_name) = s.street_name")
                all_asset_rows = self.cur.fetchall()
                
                num_assets = len(all_asset_rows)
                
                all_asset_id_list = []
                all_asset_latitude_list = []
                all_asset_longitude_list = []
                all_asset_street_id_list = []
                for row in all_asset_rows:
                        all_asset_id_list.append(row[0])
                        all_asset_latitude_list.append(row[1])
                        all_asset_longitude_list.append(row[2])
                        all_asset_street_id_list.append(row[3])
                
                #generate global neighbor for each asset
                for asset_index in range(num_assets):
                        
                        print "process asset ", asset_index
                        
                        #the input asset id
                        asset_id = all_asset_id_list[asset_index]
                        
                        #the street_id for the asset
                        street_id = all_asset_street_id_list[asset_index]
                
                        #get the lat, long for the input asset
                        latitude = all_asset_latitude_list[asset_index]
                        longitude = all_asset_longitude_list[asset_index]
                
                        #construct the lat, long tuple 
                        latlong = (latitude, longitude)
                
                        #get all assets rows in the neighborhood region                                
                        asset_rows_in_neighborhood_region = self.getAssetRowsInRegion(latlong, self.radius)
        
                        num_assets_in_neighborhood_region = len(asset_rows_in_neighborhood_region)        

                        #if the number of assets in the neighborhood region is less than 3, we will try a larger region
                        if num_assets_in_neighborhood_region < 3:
                                                
                                #get all assets rows in a larger neighborhood region                                
                                asset_rows_in_neighborhood_region = self.getAssetRowsInRegion(latlong, 2*self.radius)
        
                                num_assets_in_neighborhood_region = len(asset_rows_in_neighborhood_region)        
                        
                        
                        #if the number of assets in the larger neighborhood region is still less than 3, we will skip global neighbor update
                        if num_assets_in_neighborhood_region < 3:
                                continue                        
                                #return
                        
                        #within the neighborhood, find the two closest neighbors for the asset
                        
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
                                
                                                        
                        #if the closest distance or second closest distance is still sys.maxint, skip, don't insert into database
                        if closest_neighbor_distance == sys.maxint or second_closest_neighbor_distance == sys.maxint:
                                print "invalid distance occurs in addGlobalNeighbor, skip the record, don't insert into the global_asset_neighbors table"
                                continue
                
                        #already get the closest neighbor and second closest neighbor 
                        #store the two neighbors into the database table 'global_asset_neighbors', attributes include: 
                        #street_id, target_asset_id, target_asset_latitude, target_asset_longitude, closest_neighbor_id, closest_neighbor_distance, second_closest_neighbor_id, second_closest_neighbor_distance

                        #insert into global_asset_neighbors table
                        try:               
                                #insert neighbors of the target asset into global_asset_neighbors table
                                self.cur.execute("insert into global_asset_neighbors(street_id, asset_id, latitude, longitude, first_neighbor_id, distance_to_first_neighbor, second_neighbor_id, distance_to_second_neighbor) values (%s, %s, %s, %s, %s, %s, %s, %s)", 
                                                (street_id, target_asset_id, target_asset_latitude, target_asset_longitude, closest_neighbor_id, closest_neighbor_distance, second_closest_neighbor_id, second_closest_neighbor_distance))
                        except:
                                print "I am unable to insert into global_asset_neighbors table for street %d, asset %d." % (street_id, target_asset_id)

                
                        #commit the global neighbor update into the database        
                        self.conn.commit()
                        
                                       
        
        def run(self):
                #populate the streets table
                self.populateStreetsTable()
                
                #generate street local neighbors
                self.generateStreetLocalNeighbors()
                
                #generate global neighbors for each asset
                self.generateGlobalNeighbors()
                
                #close the database connection
                self.cur.close()
                self.conn.close()        



if __name__ == "__main__":
        
        generator = OfflineNeighborGenerator()
        generator.run()
        
        