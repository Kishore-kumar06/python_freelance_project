name = ["Buckingham Located in\n Weld County, CO", "Guernsey, Wyoming"]


for n in name:
    country_code = n.split(',')
    print(country_code)
    if not len(country_code[-1]) <= 3:
        print(n)
        