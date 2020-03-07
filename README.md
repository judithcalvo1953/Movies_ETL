# Movies_ETL Challenge Submission
## Challenge I experienced
      Having to create one large function to load my tables was difficult, and seemed to introduce other errors in the cleaning process, due in part, to my inexperience with using functions.  I need more work in this area.  While I was able to get a final product that could be uploaded without load errors, I don't think my final movies_df data frame was as clean as the one I produced using the steps learned in the module.  Therefore, I included two files: 1) Challenge_final.py, which is the final one that runs correctly using functions such as the ETL function, and 2) Module_runs.ipynb, which is the completion of the module up to the end.  It too, runs correctly, but I think the movies_df table is cleaner.

### Assumptions
The challenge asked to provide five assumptions of the ETL process that was undertaken.  I was not entirely sure what this was driving at, but I think it referred to the qualities of the data and a critical eye to the process that occurred using the Python script. 

Therefore, these are the ones I can think of:
1: Data characteristics:
        Data is often very messy when retrieved from sites such as Wikipedia, less so with Kaggle. Nevertheless, one must rely on the functions used to extract and transform the data. If the major ETL function designed at the beginning followed the steps learned in the module about extraction and cleaning and all of the required dependencies were pre-loaded, it can be expected that we will get usable, fair to good data.
2: Transforms:
        It is assumed that the transformations used as learned in the module are performing what is expected; if there are no errors in syntax and if run-time errors are trapped using try-except judiciously, we can expect to clean the data and have it properly configured for the user who will be working with the SQL tables in the PostgreSQL database.  We have done our best to clean data of erroneous entries and superflous formatting that would make later analysis difficult or would yield incorrect results.
3: Merging of tables:
        The merge of Wiki and Kaggle data were done to maximize the efficiency of the final movies_df data frame and make it easier to use. Having only columns with 90% of more complete data and removing extraneous columns gives the end user a better analytic product.
4: Load process to SQL:
        Creation of functions, such as the ETL function, should work to allow for future automated uploads and to minimize errors that will cause data loss.  Having a judicious use of try-except blocks helps to catch errors at run-time.  Such try-except blocks should be strategically placed where errors could grind the process of loading to a halt and cause fatal errors.  You don't want too many of these because they can slow down the function's completion.
5: Checks and balances in the actual upload process to SQL:        
      In my company's experience with the ETL we use to extract data from our EMR system and put it up on a cloud for the analysis team to use, we have found that having an error-check system in place (not of my own design, but our contractor's) alerts us to the events when either full or incremental ETLS have failed.  The chunking of data process used with chunksize argument help the user to know the progress of the upload and to see where a failure occurred.  You can trust the process more when you know there are error checks in place.
