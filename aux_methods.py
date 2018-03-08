def get_pincode(lat, lng):
	sensor = 'true'
	base = "http://maps.googleapis.com/maps/api/geocode/json?"
	params = "latlng={lat},{lon}&sensor={sen}".format(lat=lat,lon=lng,sen=sensor)
	url = "{base}{params}".format(base=base, params=params)
	response = requests.get(url).json()
	results = response["results"]
	address_components = list(map(lambda x: x["address_components"], results))
	flatten = [item for sublist in address_components for item in sublist]
	postal_objects = list(filter(lambda x: x["types"] == ["postal_code"], flatten))
	postal_codes = list(map(lambda x: x["long_name"], postal_objects))
	return str(postal_codes[0])