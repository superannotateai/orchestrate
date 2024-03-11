"""
Python Version Compatibility: 3.8, 3.9, 3.10, 3.11
Dependencies:
    superannotate library, version 4.4.19 or higher

This script keeps track of how many times an item's status is set to `Returned` and stores the total count in the custom fields.


Before running the script, make sure to set the following environment variables:
- SA_TOKEN: The SuperAnnotate SDK token.

You can define key-value variables from the Secrets page of the Actions tab in Orchestrate. You can then mount this secret to a custom action in your pipeline.

Please refer to the documentation for more details:
https://doc.superannotate.com/docs/create-automation#secrets.

The `handler` function  triggers the script upon an event [Item annotation status updated].

"""
from superannotate import SAClient


sa = SAClient()


def handler(event, context):
    # Get project and item data
    current_state = context['after']
    item_name = current_state['name']

    project_name = sa.get_project_by_id(current_state['project_id'])['name']
    folder_name = sa.get_folder_by_id(current_state['project_id'], current_state['folder_id'])['name']

    # Sets the project path right depending on the folder name
    if folder_name == "root":
        item_path = project_name
    else:
        item_path = f"{project_name}/{folder_name}"

    # Extract existing metadata with the custom metadata
    metadata = sa.get_item_metadata(
        project=item_path,
        item_name=item_name,
        include_custom_metadata=True
    )
    # Gets the amount of times the item has been returned,
    # or if the metadata field does not exist create it
    try:
        returned = metadata["custom_metadata"]["times_returned"]
    except KeyError:
        sa.create_custom_fields(
            project=project_name,
            fields={
                "times_returned": {
                    "type": "number"
                }
            }
        )
        returned = 0

    # Add 1 more to the times the item has been returned
    returned += 1
    # Update item custom metadata
    item_values = [{item_name: {"times_returned": returned}}]
    print(f"{item_name} has been returned {str(returned)} times")
    sa.upload_custom_values(project=project_name, items=item_values)
