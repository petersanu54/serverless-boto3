import boto3
from io import BytesIO, StringIO
from PIL import Image, ImageOps
import os

size = int(os.environ['THUMBNAIL_SIZE'])

s3 = boto3.client("s3")
""":type: pyboto3.s3"""


################################################Main function#######################################
def s3_thumbnail_function(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key_name = event['Records'][0]['s3']['object']['key']
    if not key_name.endswith("_thumbnail.png"):

        # first download the image
        image = get_image(bucket_name, key_name)

        # create the thumbnail of the image
        thumbnail = image_to_thumbnail(image)

        # give filename to new thumbnail
        new_key_name = rename_thumbnail(key_name)

        # upload the thumbnail to bucket
        url = upload_thumbnail(thumbnail, bucket_name, new_key_name)
        return url


########################################download real size image#####################################
def get_image(bucket_name, key_name):
    response = s3.get_object(Bucket=bucket_name, Key=key_name)
    image_content = response['Body'].read()
    file = BytesIO(image_content)
    img = Image.open(file)
    return img


#######################################create the thumbnail of the image#############################
def image_to_thumbnail(image):
    return ImageOps.fit(image, (size, size), Image.LANCZOS)


#######################################rename thumbnail file#########################################
def rename_thumbnail(key_name):
    key_split = key_name.split('.', 1)
    return key_split[0] + '_thumbnail.png'


#######################################upload new thumbnail to s3####################################
def upload_thumbnail(thumbnail, bucket_name, new_key_name):
    out_thumbnail = BytesIO()
    thumbnail.save(out_thumbnail, 'PNG')
    out_thumbnail.seek(0)
    s3.put_object(
        ACL="public-read-write",
        Body=out_thumbnail,
        Bucket=bucket_name,
        Key=new_key_name,
        ContentType="image/png"
    )
    url_thumbnail = '{}/{}/{}'.format(s3.meta.endpoint_url, bucket_name, new_key_name)
    return url_thumbnail
