import requests


class CitiesHandlers:
    
    @staticmethod
    def clean_withsape(list_of_city:list):
        """
        A function to replace any whitespaces by "+" sign from a list of string
        return the same list of string
        """
        return [x.replace(" ", "+") for x in list_of_city] 
    
    @staticmethod
    def generate_id(list_of_city:list):
        """
        A function to generate a unique id for each city to scrap
        return a dictionary list with the unique ID and city name  
        """
        reformatted_city = []
        for city in list_of_city:
            item = {
                'id':abs(hash(city)) % (10 ** 8),
                'city_to_scrap': city
            }
            reformatted_city.append(item)
        return reformatted_city

            

class ApiCaller:
    
    @staticmethod
    def get_weather_data(lat:float, long:float):
        """
        Method for getting weather based on latitude and longitude. 
        """
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={long}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=Europe%2FBerlin&start_date=2022-10-04&end_date=2022-10-11"
        response = requests.request("GET", url)
        return response.json()
        
    
    @staticmethod
    def get_lat_long(city_id, city_name):
                
        """
        Method for getting city lat and long
        """
        BASE_URL = "https://nominatim.openstreetmap.org/search"
        querystring = {"q":f"{city_name}","county":"france","format":"json","limit":"1"}
        response = requests.request("GET", BASE_URL, params=querystring)
        data = response.json()
        # return data
        return city_id, round(float(data[0]['lat']),2), round(float(data[0]['lon']),2), data[0]['display_name']



