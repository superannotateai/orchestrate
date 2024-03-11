"""
Python Version Compatibility: 3.9, 3.10, 3.11
Dependencies:
    superannotate library, version 4.4.19 or higher
    shapely library, version 2.0.3 or higher
    numpy library, version 1.26.2 or higher

This script checks if polygons overlap and are complex (e.g. self-intersecting polygons). It then leaves a comment on the item with the corresponding error message.

Before running the script, make sure to set the following environment variables:
- SA_TOKEN: The SuperAnnotate SDK token.

You can define key-value variables from the Secrets page of the Actions tab in Orchestrate. You can then mount this secret to a custom action in your pipeline.

Please refer to the documentation for more details: https://doc.superannotate.com/docs/create-automation#secrets.

The `handler` function triggers the script upon an event [Item annotation status updated].

"""

from superannotate import SAClient
from shapely.errors import ShapelyError
from shapely.geometry import Polygon, LinearRing
from shapely.validation import explain_validity
import numpy as np

sa = SAClient()

# User email to be used for comments
# change to your preferred one
USER_EMAIL = "my@user.com"


def handler(event, context):
    # Get project and folder information from the event
    current_state = context['after']
    image_name = current_state["name"]
    project_id, folder_id = current_state["project_id"], current_state["folder_id"]
    project_name = sa.get_project_by_id(project_id)['name']
    folder_name = sa.get_folder_by_id(project_id=project_id, folder_id=folder_id)['name']

    # Get the right project path dependent on folder name
    if folder_name == "root":
        item_path = project_name
    else:
        item_path = f"{project_name}/{folder_name}"

    # Get the annotation to memory
    annotation = sa.get_annotations(item_path, items=[image_name])[0]

    # Get all polygons in the image
    all_polygons = []
    self_ints = []

    # Iterate over all instances
    for instance in annotation["instances"]:
        # If the instance is a polygon proceed
        if instance["type"] == "polygon":
            points = instance["points"]
            pts = np.reshape(points, (-1, 2))

            # Get polygon points in shapely format
            polygon = Polygon(pts)
            lr = LinearRing(pts)

            # Complex polygon shape, like self-intersecting
            if not lr.is_simple:
                self_ints.append(1)
                sample = explain_validity(lr)
                coordinates = sample[sample.index("[") + 1: sample.index("]")]
                # Get point where issue exist
                x, y = map(float, coordinates.split())
                # Append a comment for the issue at the location
                annotation["comments"].append(
                    {
                        "correspondence": [
                            {"text": "Polygon Self-Intersecting", "email": USER_EMAIL}
                        ],
                        "x": x,
                        "y": y,
                        "resolved": False,
                    }
                )

            if lr.is_valid:
                all_polygons.append(polygon)
            else:
                all_polygons.append(polygon.buffer(0))

    total = len(all_polygons)

    # Go over polygons in pairs to check if they overlap
    overlapped = []
    for i in range(total - 1):
        for j in range(i + 1, total):
            # Get polygon pair
            p1 = all_polygons[i]
            p2 = all_polygons[j]
            if p1.intersects(p2):
                p = sorted([i, j])
                # Double check area becomes larger with intersect
                if p1.area + p2.area < p1.area + p2.area + p1.intersection(p2).area:
                    overlapped.append(p)
    # Add the polygons that overlap to comment
    to_comment = []
    [to_comment.append(i) for i in overlapped if i not in to_comment]
    # Loop over polygon pairs to comment
    for i in to_comment:
        id1 = i[0]
        id2 = i[1]
        # Try and get the polygon coordinates for overlap, some geometries need special case
        try:
            coord = all_polygons[id1].intersection(all_polygons[id2]).exterior.coords[0]
        except (ShapelyError, IndexError):
            coord = list(all_polygons[id1].intersection(all_polygons[id2]).geoms)[
                0
            ].exterior.coords[0]
        # Add a comment with the location of the intersection
        annotation["comments"].append(
            {
                "correspondence": [{"text": "Polygons overlap", "email": USER_EMAIL}],
                "x": coord[0],
                "y": coord[1],
                "resolved": False,
            }
        )

    # Upload annotations with comments and return the image
    if len(to_comment) or self_ints:
        # Upload annotation, i.e. add comments
        sa.upload_annotations(f"{project_name}{folder_name}",
                              annotations=[annotation],
                              keep_status=True)

        # Change annotation status to returned to be fixed
        sa.set_annotation_statuses(
            f"{project_name}{folder_name}", "Returned", [image_name]
        )
