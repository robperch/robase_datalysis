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

        ## For ML 
        'feature_type': ([string] feature type for feature engineering (categoric, numeric)),
        'model_relevant': ([boolean] indicating if the feature will be fed to the model),
        'id_feature': ([boolean] boolean to indicate if this is the id feature in the dataset),
        'predict_label': ([boolean] boolean to indicate if this is the predict label in the dataset),

    }

}
```
