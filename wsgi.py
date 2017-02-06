# Use this file for development, on a production setup (eg a CKAN production
# install) use deployment/datapusher.wsgi

from ckanserviceprovider import web
from spatialingestor import jobs

web.init()

# check whether jobs have been imported properly
assert jobs.spatial_ingest
assert jobs.spatial_purge

web.app.run(web.app.config.get('HOST'), web.app.config.get('PORT'))