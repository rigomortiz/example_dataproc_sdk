# EJERCICIOS

Archivo csv
````csv
"name"|"country"|"bike_id"|"purchase_year"|"purchase_city"|"purchase_continent"|"purchase_online"
"Fernando"|"MX"|"13"|"2015"|"CDMX"|"America"|"true"
"Fernando"|"MX"|"13"|"2016"|"CDMX"|"America"|"false"
"Fernando"|"MX"|"13"|"2017"|"CDMX"|"America"|"false"
"Fernando"|"MX"|"13"|"2018"|"CDMX"|"America"|"false"
"Fernando"|"MX"|"13"|"2019"|"CDMX"|"America"|"true"
"Fernando"|"MX"|"13"|"2020"|"CDMX"|"America"|"true"
"Fernando"|"MX"|"13"|"2021"|"CDMX"|"America"|"false"
"Noe"|"MX"|"16"|"2010"|"Hidalgo"|"America"|"false"
"Noe"|"MX"|"16"|"2013"|"Hidalgo"|"America"|"false"
"Noe"|"MX"|"16"|"2016"|"Hidalgo"|"America"|"false"
"Noe"|"MX"|"16"|"2019"|"Hidalgo"|"America"|"false"
"Noe"|"MX"|"16"|"2021"|"Hidalgo"|"America"|"false"
"Vincent"|"MX"|"9"|"2015"|"Edo. Mex"|"America"|"false"
"Vincent"|"MX"|"9"|"2018"|"Edo. Mex"|"America"|"true"
"Vincent"|"MX"|"9"|"2021"|"Edo. Mex"|"America"|"true"
"Anilu"|"FL"|"16"|"2016"|"Filipinas"|"Asia"|"false"
"Anilu"|"FL"|"16"|"2017"|"Filipinas"|"Asia"|"false"
"Anilu"|"FL"|"16"|"2018"|"Filipinas"|"Asia"|"false"
"Anilu"|"FL"|"16"|"2019"|"Filipinas"|"Asia"|"false"
"Luis Angel"|"ES"|"7"|"2020"|"España"|"Europa"|"true"
"Luis Angel"|"ES"|"7"|"2020"|"España"|"Europa"|"true"
"Luis Angel"|"ES"|"7"|"2020"|"España"|"Europa"|"true"
"Luis Fernando"|"TX"|"6"|"2010"|"Texas"|"America"|"true"
"Luis Fernando"|"TX"|"6"|"2011"|"Texas"|"America"|"true"
"Luis Fernando"|"TX"|"6"|"2012"|"Texas"|"America"|"true"
"Luis Fernando"|"TX"|"6"|"2013"|"Texas"|"America"|"true"
"Luis Fernando"|"TX"|"6"|"2014"|"Texas"|"America"|"true"
"Luis Fernando"|"MX"|"6"|"2015"|"Edo. Mex"|"America"|"false"
"Luis Fernando"|"MX"|"6"|"2016"|"Edo. Mex"|"America"|"false"
"Luis Fernando"|"MX"|"6"|"2017"|"Edo. Mex"|"America"|"false"
"Luis Fernando"|"MX"|"6"|"2018"|"Edo. Mex"|"America"|"false"
"Luis Fernando"|"MX"|"6"|"2019"|"Edo. Mex"|"America"|"false"
"Ana"|"IT"|"13"|"2018"|"Roma"|"Europa"|"false"
"Ana"|"TK"|"13"|"2019"|"Tokyo"|"Asia"|"false"
"Ana"|"MX"|"13"|"2020"|"Edo. Mex"|"America"|"false"
"Jessica"|"TX"|"2"|"2015"|"Texas"|"America"|"true"
"Jessica"|"TX"|"2"|"2016"|"Texas"|"America"|"false"
"Jessica"||"2"|"2017"||"America"|"true"
"Jessica"||"2"|"2018"||"America"|"false"
````

0. Crea una columna 'abr' con la columna "country" en minúsculas filtrado por "purchase_continent" igual a America
````scala
df.select(df.columns.map {
  case name: String if name == Country.name => lower(Country.column).alias(ABR)
  case _@name => col(name)
} :+ lit(message).alias(MESSAGE_COL) :_*)
  .filter(PurchaseContinent.column === AMERICA)
  .drop(PurchaseOnline.column)
````

1.Filtrar la tabla t_fdev_customer, conservar solo los registrosque tengan una fecha de compra(purchase_year)
mayor a current_day - 10 years y que hayan sido realizadas(purchase_city) en una ciudad diferente a tokio
Nota current_date no debe de ser un valor en duro
3. 