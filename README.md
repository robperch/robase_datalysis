# Template repository for data analysis

## Objective
Simplify the creation of an environment with the tools required to conduct an effective data analysis

## Data schema structure
```
data_schema = {

    'feature': {

        ## For general data wrangling functions
        'relevant': ([boolean] indicating if the feature is eliminated in a first phase),
        'clean_col_name': ([string] string with the new defined name for the feature),
        'data_type': ([string] data type indicating to format the feature (str, int, float, datetime)),
        'value_map': ([dict] dict with keys as original name and value as the substitution),

    }

}
```
