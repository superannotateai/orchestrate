# Integration for Pushing Data from SuperAnnotate to Snowflake
## Introduction
This Python action is designed to facilitate the seamless integration and transfer of data to **Snowflake**. It simplifies the process of uploading datasets from various sources directly into your Snowflake DB, making data management more efficient and less error-prone.
## Action Parameters
Before you begin using this action, ensure you have following action parameters in your **Orchestrate** pipeline:
## Usage
1. In your SuperAnnotate team, [create a Secret](https://doc.superannotate.com/docs/create-automation#secrets) with following values:
    - `SNOWFLAKE_PRIVATE_KEY`: Snowflake Private Key [personal access token](https://docs.snowflake.com/en/user-guide/key-pair-auth),
    - `SA_TOKEN`: SuperAnnotate [token for Python SDK](https://doc.superannotate.com/docs/token-for-python-sdk)
2. In your SuperAnnotate team, [create an action](https://doc.superannotate.com/docs/pipeline-components#actions) using the `Insert data into Snowflake Table` action template.
3. In your SuperAnnotate project [create a pipeline](https://doc.superannotate.com/docs/create-automation#create-a-pipeline)
    - From the **Item** section under **Event**, drag and drop a **Fired in Explore** event onto the canvas.
    - Next, drag and drop the action created in step 2.
    - Finally, link the event to the action.
4. Click on the action to configure its properties:
    - Set the secret value as the one created in step 1.
    - Enter the following values into the **Event object**:
        - `SA_COMPONENT_IDS`: String with component IDs, separated by commas, which should move to Snowflake
        - `SNOWFLAKE_USERNAME`: Snowflake user name
        - `SNOWFLAKE_ACCOUNT`: Snowflake account
        - `SNOWFLAKE_WAREHOUSE`: Snowflake warehouse
        - `SNOWFLAKE_DATABASE`: Snowflake DB name
        - `SNOWFLAKE_DATABASE_SCHEMA`: Snowflake schema name
        - `SNOWFLAKE_DB_TABLE_PATH`: <Snowflake DB>.<Snowflake Schema>.<Snowflake>
        - `SNOWFLAKE_DB_COLUMN_NAMES`: String of column names, separated by commas, which should receive the values from SuperAnnotate (order should match)
5. Press **Save**.
Now, every time you [run a pipeline from Explore](https://doc.superannotate.com/docs/bulk-actions#run-pipeline), data from selected items will be moved to **Snowflake** automatically.