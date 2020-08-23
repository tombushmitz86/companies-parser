#### Installation:

create and run the local virtualenv

```
python3 -m venv lusha-env
source lusha/bin/activate
pip install -r requirements
```


#### Running the file:
`python companies.py`

The task outputs the following files:
* **companies_per_country_histo.png** - histogram of the companies grouped by EU countries.
* **companies_per_country.csv** - corrosponding CSV
* **comapnies_with_keywords_columns.csv** - updated dataframe with "Keywords" column 


#### Handling duplicates:
there is two approches I would take, 
I would set up front a key that would best describe a company, e.g (company name, domain, description...) 
and I would hash it (similar to checksum), then a duplicate would consider an entry that resolves to the same checksum, and should be dropped.

another approch, which is less brute force, 
is to examine changes in the key (as described above) and comparing it to a new entry (suspected duplicates) give each field differences a weight, and based on those weights decide if the entry is really considerd as a duplicate or not.

The main issue here is that the company "key", i.e its identifier on which we populate our data set, can be complex.

