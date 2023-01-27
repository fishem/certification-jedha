from .api_caller import CitiesHandlers, ApiCaller

    
class GetApidata:
    
    def __init__(self,  CITIES:list):
        self.CITIES = CITIES
    
    def _citie_transformation(self):
        self.cities = CitiesHandlers.clean_withsape(self.CITIES)
        self.cities = CitiesHandlers.generate_id(self.cities)
        return self.cities
    
    
    def get_data(self):
        """
        Call the location api (long, lat) and weather api from the list of the cities 
        and return a list of city with location and weather 
        """
        
        # Get Geolocation data from cities
        self.result_from_apis = []
        for city in self._citie_transformation():
            id_city, lat, long, city_name = ApiCaller.get_lat_long(city['id'], city['city_to_scrap'])
            weather_data = ApiCaller.get_weather_data(lat, long)
                
            output = {
                'id': id_city,
                'city_to_scrap': city_name.split(",")[0],
                'lat': lat,
                'lon': long,
                'elevation': weather_data['elevation'],
                'days': weather_data['daily']['time'],
                'temp_max': weather_data['daily']['temperature_2m_max'],
                'temp_min': weather_data['daily']['temperature_2m_min'],
                'pre_sum': weather_data['daily']['precipitation_sum']
                }
            self.result_from_apis.append(output)
        
        return self.result_from_apis







    