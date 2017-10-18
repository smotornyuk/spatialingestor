# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
import locale
import logging
import os
import re
import shutil
import socket
import tempfile
import urllib
import urllib2
import urlparse
import uuid
import zipfile
from subprocess import call

import psycopg2
import requests
from ckanserviceprovider import job, util, web
from lxml import etree
from osgeo import osr

from spatialingestor import ogr2ogr
if not locale.getlocale()[0]:
    locale.setlocale(locale.LC_ALL, '')

DOWNLOAD_TIMEOUT = 30


class HTTPError(util.JobError):
    """Exception that's raised if a job fails due to an HTTP problem."""

    def __init__(self, message, status_code, request_url, response):
        """Initialise a new HTTPError.
        :param message: A human-readable error message
        :type message: string
        :param status_code: The status code of the errored HTTP response,
            e.g. 500
        :type status_code: int
        :param request_url: The URL that was requested
        :type request_url: string
        :param response: The body of the errored HTTP response as unicode
            (if you have a requests.Response object then response.text will
            give you this)
        :type response: unicode
        """
        super(HTTPError, self).__init__(message)
        self.status_code = status_code
        self.request_url = request_url
        self.response = response

    def as_dict(self):
        """Return a JSON-serializable dictionary representation of this error.
        Suitable for ckanserviceprovider to return to the client site as the
        value for the "error" key in the job dict.
        """
        if self.response and len(self.response) > 200:
            response = self.response[:200] + '...'
        else:
            response = self.response
        return {
            "message": self.message,
            "HTTP status code": self.status_code,
            "Requested URL": self.request_url,
            "Response": response,
        }


#Generate the file paths to traverse, or a single path if a file name was given
def getfiles(path):
    if os.path.isdir(path):
        for root, dirs, files in os.walk(path):
            for name in files:
                yield os.path.join(root, name)
    else:
        yield path


def get_url(action, ckan_url):
    """
    Get url for ckan action
    """
    if not urlparse.urlsplit(ckan_url).scheme:
        ckan_url = 'http://' + ckan_url.lstrip('/')
    ckan_url = ckan_url.rstrip('/')
    return '{ckan_url}/api/3/action/{action}'.format(
        ckan_url=ckan_url, action=action)


def check_response(response, request_url, who, good_status=(201, 200), ignore_no_success=False):
    """
    Checks the response and raises exceptions if something went terribly wrong
    :param who: A short name that indicated where the error occurred
                (for example "CKAN")
    :param good_status: Status codes that should not raise an exception
    """
    if not response.status_code:
        raise HTTPError(
            'Spatial Ingestor received an HTTP response with no status code',
            status_code=None, request_url=request_url, response=response.text)

    message = '{who} bad response. Status code: {code} {reason}. At: {url}.'
    try:
        if not response.status_code in good_status:
            json_response = response.json()
            if not ignore_no_success or json_response.get('success'):
                try:
                    message = json_response["error"]["message"]
                except Exception:
                    message = message.format(
                        who=who, code=response.status_code,
                        reason=response.reason, url=request_url)
                raise HTTPError(
                    message, status_code=response.status_code,
                    request_url=request_url, response=response.text)
    except ValueError as err:
        message = message.format(
            who=who, code=response.status_code, reason=response.reason,
            url=request_url, resp=response.text[:200])
        raise HTTPError(
            message, status_code=response.status_code, request_url=request_url,
            response=response.text)


def ckan_command(command_name, data_dict, ckan_dict):
    url = get_url(command_name, ckan_dict['ckan_url'])
    r = requests.post(url,
                      data=json.dumps(data_dict),
                      headers={'Content-Type': 'application/json',
                               'Authorization': ckan_dict['api_key']}
                      )
    check_response(r, url, 'CKAN')

    return r.json()['result']


def validate_input(input):
    # Especially validate metdata which is provided by the user
    if not 'metadata' in input:
        raise util.JobError('Metadata missing')
    if not 'api_key' in input:
        raise util.JobError('CKAN API key missing')

    required_metadata_keys = {
        'resource_id',
        'ckan_url',
        'postgis',
        'geoserver',
        'geoserver_public_url',
        'target_spatial_formats'
    }

    missing_metadata_keys = required_metadata_keys - set(input['metadata'].keys())

    if missing_metadata_keys:
        raise util.JobError('Missing metadata keys: {0}'.format(missing_metadata_keys))

    required_db_metadata_keys = {
        'db_host',
        'db_name',
        'db_user',
        'db_pass'
    }

    missing_db_metadata_keys = required_db_metadata_keys - set(input['metadata']['postgis'].keys())

    if missing_db_metadata_keys:
        raise util.JobError('Missing DB metadata keys: {0}'.format(missing_db_metadata_keys))

        required_geoserver_metadata_keys = required_db_metadata_keys

    missing_geoserver_metadata_keys = required_geoserver_metadata_keys - set(input['metadata']['geoserver'].keys())

    if missing_geoserver_metadata_keys:
        raise util.JobError('Missing Geoserver metadata keys: {0}'.format(missing_geoserver_metadata_keys))


def get_spatial_input_format(resource):
    check_string = resource.get('__extras', {}).get('format', resource.get('format', resource.get('url', ''))).upper()

    if any([check_string.endswith(x) for x in ["SHP", "SHAPEFILE"]]):
        return 'SHP'
    elif check_string.endswith("KML"):
        return 'KML'
    elif check_string.endswith("KMZ"):
        return 'KMZ'
    elif check_string.endswith("GRID"):
        return 'GRID'
    else:
        raise util.JobError("Failed to determine spatial file type for {0}".format(resource.get('url', '')))


def get_db_cursor(data):
    db_port = None
    if data['postgis'].get('db_port', '') != '':
        db_port = data['postgis']['db_port']

    try:
        connection = psycopg2.connect(dbname=data['postgis']['db_name'],
                                      user=data['postgis']['db_user'],
                                      password=data['postgis']['db_pass'],
                                      host=data['postgis']['db_host'],
                                      port=db_port)
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return connection.cursor(), connection
    except Exception, e:
        raise util.JobError("Failed to connect with PostGIS with error {0}".format(str(e)))


def setup_spatial_table(data, resource_id):
    cursor, connection = get_db_cursor(data)

    table_name = "sp_" + resource_id.replace("-", "_")

    cursor.execute("DROP TABLE IF EXISTS {tab_name}".format(tab_name=table_name))
    cursor.close()
    connection.close()

    return table_name


def db_upload(data, parent_resource, input_format, table_name, logger):

    def download_file(resource, file_format):
        tmpname = None
        if 'SHP' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'shp.zip')
        elif 'KML' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'kml')
        elif 'KMZ' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'kml.zip')
        elif 'GRID' == file_format:
            tmpname = '{0}.{1}'.format(uuid.uuid1(), 'zip')

        if tmpname is None:
            raise util.JobError("Failed to recognize file format extension {0}".format(file_format))

        logger.info('Fetching from: {0}'.format(resource.get('url')))

        try:
            request = urllib2.Request(resource.get('url'))

            if resource.get('url_type') == 'upload':
                request.add_header('Authorization', data['api_key'])

            response = urllib2.urlopen(request, timeout=DOWNLOAD_TIMEOUT)
        except urllib2.HTTPError as e:
            raise HTTPError(
                "SpatialIngestor received a bad HTTP response when trying to download "
                "the data file", status_code=e.code,
                request_url=resource.get('url'), response=e.read())

        except urllib2.URLError as e:
            if isinstance(e.reason, socket.timeout):
                raise util.JobError('Connection timed out after %ss' %
                                    DOWNLOAD_TIMEOUT)
            else:
                raise HTTPError(
                    message=str(e.reason), status_code=None,
                    request_url=resource.get('url'), response=None)

        try:
            with open(os.path.join(tempdir, tmpname), 'wb') as out_file:
                out_file.write(response.read())
        except Exception, e:
            raise util.JobError("Failed to copy file to {0} with exception {1}".format(os.path.join(tempdir, tmpname), str(e)))

        return os.path.join(tempdir, tmpname)

    def unzip_file(zpf, filepath):
        # Take only the filename before the extension
        f_path, f_name = os.path.split(filepath)
        dirname = os.path.join(tempdir, f_name.split('.', 1)[0])

        try:
            os.makedirs(dirname)
        except:
            # Probably created by another process
            logger.info("Previous temp directory {0} found".format(dirname))

        for name in zpf.namelist():
            zpf.extract(name, dirname)

        return dirname

    def db_ingest(full_file_path, crs='EPSG:4326'):
        # Use ogr2ogr to process the KML into the postgis DB
        port_string = ''
        if 'db_port' in data['postgis']:
            port_string = '\' port=\'' + data['postgis']['db_port']

        args = ['', '-f', 'PostgreSQL', "--config", "PG_USE_COPY", "YES",
                'PG:dbname=\'' + data['postgis']['db_name'] + '\' host=\'' + data['postgis'][
                    'db_host'] + port_string + '\' user=\'' + data['postgis'][
                    'db_user'] + '\' password=\'' + data['postgis']['db_pass'] + '\'', full_file_path, '-lco',
                'GEOMETRY_NAME=geom', "-lco", "PRECISION=NO", '-nln', table_name, '-a_srs', crs,
                '-nlt', 'PROMOTE_TO_MULTI', '-overwrite']

        return ogr2ogr.main(pargs)

    tempdir = tempfile.mkdtemp()

    try:

        native_crs = "EPSG:4326"
        unzip_dir = None

        base_filepath = download_file(parent_resource, input_format)

        # Do we need to unzip?
        if input_format in ["KMZ", "SHP", "GRID"]:
            try:
                zpf = zipfile.ZipFile(base_filepath)
                unzip_dir = unzip_file(zpf, base_filepath)
            except:
                raise util.JobError("{0} is not a valid zip file".format(base_filepath))

            # Flatten the zip file
            for root, dirs, files in os.walk(unzip_dir):
                for sub_dir in dirs:
                    from_dir = os.path.join(root, sub_dir)
                    for f in getfiles(from_dir):
                        filename = f.split('/')[-1]
                        if os.path.isfile(os.path.join(unzip_dir, filename)):
                            filename = f.replace(from_dir, "", 1).replace("/", "_")
                        shutil.copy(f, os.path.join(unzip_dir, filename))
                    shutil.rmtree(from_dir)

            for f in os.listdir(unzip_dir):
                if f.lower().endswith(".kml"):
                    kml_file = os.path.join(unzip_dir, f)

            try:
                os.remove(os.path.join(base_filepath))
            except:
                raise util.JobError("Could not remove {0}".format(base_filepath))

        if input_format in ["KMZ", "KML", "GRID"]:
            if unzip_dir is not None:
                kml_file = None
                for f in os.listdir(unzip_dir):
                    if f.lower().endswith(".kml"):
                        kml_file = os.path.join(unzip_dir, f)

                if kml_file is None:
                    raise util.JobError("No KML file found in expanded archive {0}".format(unzip_dir))
            else:
                kml_file = base_filepath

            # Update folder name in KML file with table_name
            try:
                tree = etree.parse(kml_file)
            except IOError:
                raise util.JobError("Failed to read KML file {0}".format(kml_file))

            for ns in ['http://www.opengis.net/kml/2.2', 'http://earth.google.com/kml/2.1']:
                find = etree.ETXPath('//{' + ns + '}Folder/{' + ns + '}name')
                element = find(tree)
                for x in element:
                    x.text = table_name

            # Write new KML file
            kml_file_new = os.path.join(tempdir, table_name + ".kml")
            try:
                with open(kml_file_new, 'w') as out_file:
                    out_file.write(etree.tostring(tree))
            except:
                raise util.JobError("Failed to write KML file {0}".format(kml_file_new))

            # Use ogr2ogr to process the KML into the postgis DB
            return_code = db_ingest(kml_file_new, native_crs)

            # Remove edited KML file
            try:
                os.remove(kml_file_new)
            except:
                if return_code == 1:
                    raise util.JobError(
                        "Ogr2ogr failed to ingest KML file {0} into PostGIS DB and failed to said KML file".format(
                            kml_file_new))
                else:
                    raise util.JobError("Failed to delete KML file {0}".format(kml_file_new))

            if return_code == 1:
                raise util.JobError("Ogr2ogr failed to ingest KML file {0} into PostGIS DB".format(kml_file_new))
            else:
                logger.info("Ogr2ogr successfully ingested KML file {0} into PostGIS DB".format(kml_file_new))

        elif input_format == "SHP":

            shp_file = None
            prj_file = None
            for f in os.listdir(unzip_dir):
                if f.lower().endswith(".shp"):
                    shp_file = f

                if f.lower().endswith(".prj"):
                    prj_file = f

            if shp_file is None:
                raise util.JobError("Failed to find shapefile in archive {0}".format(base_filepath))

            file_path = os.path.join(unzip_dir, shp_file)

            # Determine projection information
            if prj_file:
                try:
                    prj_txt = open(os.path.join(unzip_dir, prj_file), 'r').read()
                except:
                    raise util.JobError("Failed to open projection file {0}".format(prj_file))

                sr = osr.SpatialReference()
                sr.ImportFromESRI([prj_txt])
                res = sr.AutoIdentifyEPSG()
                if res == 0:  # Successful auto-identify
                    native_crs = sr.GetAuthorityName(None) + ":" + sr.GetAuthorityCode(None)
                elif any(x in prj_txt for x in ["GDA_1994_MGA_Zone_56", "GDA94_MGA_zone_56"]):
                    native_crs = "EPSG:28356"
                elif any(x in prj_txt for x in ["GDA_1994_MGA_Zone_55", "GDA94_MGA_zone_55"]):
                    native_crs = "EPSG:28355"
                elif any(x in prj_txt for x in ["GDA_1994_MGA_Zone_54", "GDA94_MGA_zone_54"]):
                    native_crs = "EPSG:28354"
                elif "GCS_GDA_1994" in prj_txt:
                    native_crs = "EPSG:4283"
                elif 'GEOGCS["GDA94",DATUM["D_GDA_1994",SPHEROID["GRS_1980"' in prj_txt:
                    native_crs = "EPSG:4283"
                elif "MapInfo Generic Lat/Long" in prj_txt:
                    native_crs = "EPSG:4326"
                elif "Asia_South_Equidistant_Conic" in prj_txt:
                    native_crs = "ESRI:102029"
                elif "Australian_Albers_Equal_Area_Conic_WGS_1984" in prj_txt:
                    native_crs = "EPSG:3577"
                elif "WGS_1984_Web_Mercator_Auxiliary_Sphere" in prj_txt:
                    native_crs = "EPSG:3857"
                elif 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984"' in prj_txt:
                    native_crs = "EPSG:4326"
                else:
                    logger.info()
                    raise util.JobError("Failed to identify projection {0} for {1}".format(prj_txt, file_path))

                logger.info("Successfully identified projection of {0} as {1}".format(parent_resource['url'], native_crs))

            # Use ogr2ogr to process the shapefile into the postgis DB
            return_code = db_ingest(file_path, native_crs)

            if return_code == 1:
                raise util.JobError("Ogr2ogr failed to ingest shapefile {0} into PostGIS DB".format(file_path))
            else:
                logger.info("Ogr2ogr successfully ingested shapefile file {0} into PostGIS DB".format(file_path))

        else:
            # Should never get here
            raise util.JobError("Failed to recognize file format extension {0}".format(input_format))
    finally:
        try:
            shutil.rmtree(tempdir)
        except:
            pass

    return native_crs


def geoserver_transfer(data, package, input_format, native_crs, table_name, logger):
    # Call to postgis to determine bounding box
    conn_params = get_db_cursor(data)
    (db_cursor, db_connection) = conn_params

    if input_format in ['KML', 'KMZ', 'GRID']:
        try:
            db_cursor.execute("ALTER TABLE {tab_name} "
                              "DROP \"description\" RESTRICT, "
                              "DROP \"timestamp\" RESTRICT, "
                              "DROP \"begin\" RESTRICT, "
                              "DROP \"end\" RESTRICT, "
                              "DROP altitudemode RESTRICT, "
                              "DROP tessellate RESTRICT, "
                              "DROP extrude RESTRICT, "
                              "DROP visibility RESTRICT, "
                              "DROP draworder RESTRICT, "
                              "DROP icon RESTRICT;".format(tab_name=table_name))
        except Exception, e:
            logger.info("Failed to alter KML PostGIS table with exception: {1}, continuing on...".format(str(e)))

    # Pull out data from PostGIS as GeoJSON, along with bounding box. Note we extract the data in a
    # "EPSG:4326" native SRS
    try:
        db_cursor.execute("SELECT ST_Extent(geom) AS box,"
                          "ST_Extent(ST_Transform(geom,4326)) AS latlngbox, "
                          "ST_AsGeoJSON(ST_Extent(ST_Transform(geom,4326))) AS geojson "
                          "FROM {tab_name}".format(tab_name=table_name))
        (bbox, latlngbbox, bgjson) = db_cursor.fetchone()
        db_cursor.close()
        db_connection.close()
    except Exception, e:
        raise util.JobError("Failed to extract data from PostGIS with exception: {0}".format(str(e)))

    # Construct geoserver url & name core
    data['geoserver_internal_url'] = 'http://' + data['geoserver']['db_host']
    if data['geoserver'].get('db_port', '') != '':
        data['geoserver_internal_url'] += ':' + data['geoserver']['db_port']
    data['geoserver_internal_url'] += '/' + data['geoserver']['db_name'] + '/'

    core_name = package['name'] + "_" + data['resource_id']
    headers = {'Content-type': 'application/json'}
    credentials = (data['geoserver']['db_user'], data['geoserver']['db_pass'])

    # Create a workspace metadata
    workspace = "ws_" + core_name
    wsurl = data['geoserver_internal_url'] + 'rest/workspaces'
    wsdata = json.dumps({'workspace': {'name': workspace}})

    # Create datastore metadata
    datastore = "ds_" + core_name
    dsurl = data['geoserver_internal_url'] + 'rest/workspaces/' + workspace + '/datastores'
    dsdata = json.dumps({'dataStore': {'name': datastore,
                                       'connectionParameters': {
                                           "dbtype": "postgisng",
                                           "encode functions": "false",
                                           "jndiReferenceName": "java:comp/env/jdbc/postgres",
                                           "Support on the fly geometry simplification": "true",
                                           "Expose primary keys": "false",
                                           "Estimated extends": "false"
                                       }}})

    # Create layer metadata
    layer = "ft_" + core_name
    fturl = data[
                'geoserver_internal_url'] + 'rest/workspaces/' + workspace + '/datastores/' + datastore + "/featuretypes"
    ftdata = {'featureType': {'name': layer, 'nativeName': table_name, 'title': package['title']}}

    bbox_obj = None
    if bbox:
        (minx, miny, maxx, maxy) = bbox.replace("BOX", "").replace("(", "").replace(")", "").replace(",",
                                                                                                     " ").split(" ")
        bbox_obj = {'minx': minx, 'maxx': maxx, 'miny': miny, 'maxy': maxy}
        (llminx, llminy, llmaxx, llmaxy) = latlngbbox.replace("BOX", "").replace("(", "").replace(")", "").replace(
            ",", " ").split(" ")
        llbbox_obj = {'minx': llminx, 'maxx': llmaxx, 'miny': llminy, 'maxy': llmaxy}

        ftdata['featureType']['nativeBoundingBox'] = bbox_obj
        ftdata['featureType']['latLonBoundingBox'] = llbbox_obj

        if float(llminx) < -180 or float(llmaxx) > 180:
            raise util.JobError("{0} has invalid automatic projection {1}, {4}".format(package['title'], native_crs))
        else:
            ftdata['featureType']['srs'] = native_crs
            if 'spatial' not in package or package['spatial'] != bgjson:
                package['spatial'] = bgjson
                ckan_command('package_update', package, data)

    ftdata = json.dumps(ftdata)

    # Remove any pre-existing geoserver assets
    logger.info("Purging any pre-existing Geoserver assets")

    # Manually add geoserver parameters here as requests does not hangle parameters without values
    # https://github.com/kennethreitz/requests/issues/2651
    res = requests.delete(wsurl + '/' + workspace + '?recurse=true&quietOnNotFound', auth=credentials)

    if res.status_code != 200:
        logger.info("Geoserver recursive workspace deletion of {0} failed with response {1}, continuing...".format(
            wsurl + '/' + workspace + '?recurse=true&quietOnNotFound', res))
    else:
        logger.info("Geoserver recursive workspace deletion of {0} succeeded".format(
            wsurl + '/' + workspace + '?recurse=true&quietOnNotFound'))

    # Upload new geoserver assets
    logger.info("Uploading new assets to Geoserver")

    res = requests.post(wsurl, data=wsdata, headers=headers, auth=credentials)

    if res.status_code != 201:
        logger.info("Geoserver workspace creation of {0} failed with response {1}, continuing...".format(wsurl, res))
    else:
        logger.info("Geoserver workspace creation of {0} succeeded".format(wsurl))

    res = requests.post(dsurl, data=dsdata, headers=headers, auth=credentials)

    if res.status_code != 201:
        logger.info("Geoserver datastore creation of {0} failed with response {1}, continuing...".format(dsurl, res))
    else:
        logger.info("Geoserver datastore creation of {0} succeeded".format(dsurl))

    res = requests.post(fturl, data=ftdata, headers=headers, auth=credentials)

    if res.status_code != 201:
        logger.info("Geoserver feature type creation of {0} failed with response {1}, continuing...".format(fturl, res))
    else:
        logger.info("Geoserver feature type creation of {0} succeeded".format(fturl))

    return workspace, layer, bbox_obj


def purge_legacy_spatial(data, package, logger):
    logger.info("Removing legacy spatial ingestor resources")

    # Delete any legacy resources
    num_deleted = 0
    pkg_names = set()
    for res in package['resources']:
        if "http://data.gov.au/geoserver/" in res.get('url', '') and all(
                [res.get(x, '') == '' for x in ['spatial_parent', 'spatial_child_of']]):
            try:
                name_search = re.search('http://data.gov.au/geoserver/(.*)/.*', res.get('url', ''), re.IGNORECASE)
                ckan_command('resource_delete', res, data)
                num_deleted += 1

                if name_search:
                    pkg_names.add(name_search.group(1))
            except:
                pass

    if num_deleted > 0:
        logger.info("Successfully deleted {0} legacy spatial ingestor resources".format(num_deleted))
    else:
        logger.info("Found no legacy spatial ingestor resources to delete")

    # Drop legacy datastore table if it exists
    try:
        logger.info("Dropping legacy PostGIS table, if it exists")

        db_port = None
        if data['postgis'].get('db_port', '') != '':
            db_port = data['postgis']['db_port']

        connection = psycopg2.connect(dbname=data['postgis']['db_name'],
                                      user=data['postgis']['db_user'],
                                      password=data['postgis']['db_pass'],
                                      host=data['postgis']['db_host'],
                                      port=db_port)
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        cursor.execute("DROP TABLE IF EXISTS {tab_name}".format(tab_name=package['id'].replace("-", "_")))
        cursor.close()
        connection.close()
    except:
        pass

    # Delete workspace, datastore and any feature types associated with the legacy dataset
    try:
        logger.info("Purging legacy Geoserver assets, if they exist")

        data['geoserver_internal_url'] = 'http://' + data['geoserver']['db_host']
        if data['geoserver'].get('db_port', '') != '':
            data['geoserver_internal_url'] += ':' + data['geoserver']['db_port']
        data['geoserver_internal_url'] += '/' + data['geoserver']['db_name'] + '/'

        credentials = (data['geoserver']['db_user'], data['geoserver']['db_pass'])

        pkg_names = list(pkg_names)

        for name in pkg_names:
            wsurl = data['geoserver_internal_url'] + 'rest/workspaces/' + name

            res = requests.delete(wsurl, params={'recurse': 'true'}, auth=credentials)

            if res.status_code != 200:
                logger.info("Found no legacy Geoserver assets for {0}".format(name))
            else:
                logger.info("Successfully purged legacy Geoserver assets for {0}".format(name))
    except:
        pass

    return True




def get_spatial_upload_formats(data, package, input_format):
    '''
    :param context:
    :param package:
    :param parent_resource:
    :param input_format:
    :return:
    '''
    result = []
    for res_format in data['target_spatial_formats']:
        # Obviously do not expand into our own format
        if res_format == input_format:
            continue

        target_id = None
        for resource in package['resources']:
            test_string = (resource['format'] + resource['url'].replace('wms?', '').replace('wfs?', '')).upper()
            if res_format in test_string or all(["JSON" in x for x in [res_format, test_string]]):
                # Format is previously existing, see if the spatial ingestor was the last editor
                if resource.get('spatial_child_of', '') == data['resource_id']:
                    # Found resource previously created by ingestor, we add its list to the ids to be modified
                    target_id = resource['id']
                    break

        result += [(res_format, target_id)]

    return result


def create_or_update_resources(data, package, parent_resource, bbox_obj, expansion_formats, layer, workspace, logger):
    ws_addr = data['geoserver_public_url'] + "/" + workspace + "/"

    number_updated = 0

    for new_format, old_id in expansion_formats:
        number_updated += 1

        resource_command = 'resource_create'
        if old_id is not None:
            resource_command = 'resource_update'
            new_res = ckan_command('resource_show', {'id': old_id}, data)
            new_res['id'] = old_id
            new_res['format'] = new_format.lower()
            new_res['spatial_child_of'] = data['resource_id']
            new_res['parent_resource_url'] = parent_resource['url']
        else:
            new_res = {'package_id': package['id'],
                       'format': new_format.lower(),
                       'spatial_child_of': data['resource_id'],
                       'parent_resource_url': parent_resource['url']}

        if new_res['format'] == 'json':
            new_res['format'] = 'geojson'

        if new_format in ["IMAGE/PNG", 'KML']:
            new_res['url'] = ws_addr + "wms?request=GetMap&layers=" + layer + "&bbox=" + bbox_obj['minx'] + "," + \
                             bbox_obj['miny'] + "," + bbox_obj['maxx'] + "," + bbox_obj[
                                 'maxy'] + "&width=512&height=512&format=" + urllib.quote(new_format.lower())
            if new_format == "IMAGE/PNG":
                new_res['name'] = package['title'] + " Preview Image"
                new_res['description'] = "View overview image of this dataset"
            elif new_format == "KML":
                new_res['name'] = package['title'] + " KML"
                new_res[
                    'description'] = "View a map of this dataset in web and desktop spatial data tools including Google Earth"
        elif new_format == "WMS":
            new_res['url'] = ws_addr + "wms?request=GetCapabilities"
            new_res['name'] = package['title'] + " - Preview this Dataset (WMS)"
            new_res['description'] = "View the data in this dataset online via an online map"
            new_res['wms_layer'] = layer
        elif new_format == "WFS":
            new_res['url'] = ws_addr + "wfs"
            new_res['name'] = package['title'] + " Web Feature Service API Link"
            new_res['description'] = "WFS API Link for use in Desktop GIS tools"
            new_res['wfs_layer'] = layer
        elif new_format in ['CSV', 'JSON', 'GEOJSON']:
            if new_format == 'CSV':
                serialization = 'csv'
                new_res['name'] = package['title'] + " CSV"
                new_res['description'] = "For summary of the objects/data in this collection"
            else:
                serialization = 'json'
                new_res['name'] = package['title'] + " GeoJSON"
                new_res['description'] = "For use in web-based data visualisation of this collection"

            new_res[
                'url'] = ws_addr + "wfs?request=GetFeature&typeName=" + layer + "&outputFormat=" + urllib.quote(
                serialization)
        else:
            continue

        try:
            ckan_command(resource_command, new_res, data)
        except Exception:
            number_updated -= 1
            logger.info("Failed to create CKAN resource {0} via API, continuing...".format(new_res['name']))

    return number_updated

@job.async
def spatial_ingest(task_id, input):
    handler = util.StoringHandler(task_id, input)
    logger = logging.getLogger(task_id)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    validate_input(input)

    data = input['metadata']
    data['api_key'] = input['api_key']

    logger.info('Retrieving resource information')

    resource = ckan_command('resource_show', {'id': data['resource_id']}, data)

    logger.info('Retrieving package information')

    package = ckan_command('package_show', {'id': resource['package_id']}, data)

    logger.info('Purging any legacy spatial ingestor assets')

    # Make sure there are no legacy resources or artifacts
    purge_legacy_spatial(data, package, logger)

    # Get package data again in case another thread deleted some legacy resources
    package = ckan_command('package_show', {'id': resource['package_id']}, data)

    # We have an ingestible resource that has been updated, passing all blacklist checks
    # and we have potential resources for creation.
    logger.info('Setting up PostGIS table for spatial assets')

    table_name = setup_spatial_table(data, data['resource_id'])

    # Determine input format
    logger.info('Determining input format for resource')

    input_format = get_spatial_input_format(resource)

    # Ingest into DB and exit if this fails for whatever reason
    logger.info('Ingesting spatial asset into PostGIS DB')

    native_crs = db_upload(data, resource, input_format, table_name, logger)

    # Create Geoserver assets for PostGIS table
    logger.info('Creating Geoserver assets for PostGIS table')

    workspace, layer, bbox_obj = geoserver_transfer(data, package, input_format, native_crs, table_name, logger)

    # Figure out if any target formats are available to be expanded into.
    # I.e. if a resource of a target format already exists and is _not_
    # last modified by the spatial ingestor user, we do not added/update the
    # resource for that format.
    expansion_formats = get_spatial_upload_formats(data, package, input_format)
    if not expansion_formats:
        raise util.JobError("Package {0} has no available formats to expand into".format(package['name']))

    logger.info("Creating CKAN resources for new Geoserver assets")

    num_update = create_or_update_resources(data, package, resource, bbox_obj, expansion_formats, layer, workspace,
                                            logger)

    logger.info("{0} resources successfully created/updated".format(num_update))


@job.async
def spatial_purge(task_id, input):
    #CKAN resource deletions are handled by client threads, we only purge PostGIS and Geoserver assets here

    handler = util.StoringHandler(task_id, input)
    logger = logging.getLogger(task_id)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    validate_input(input)

    data = input['metadata']
    data['api_key'] = input['api_key']
    print data
    logger.info(data)
    logger.info('Retrieving resource information')

    resource = ckan_command('resource_show', {'id': data['resource_id']}, data)

    logger.info('Dropping PostGIS table from DB')

    setup_spatial_table(data, data['resource_id'])

    logger.info('Purging Geoserver assets')

    # Construct geoserver url & name core
    data['geoserver_internal_url'] = 'http://' + data['geoserver']['db_host']
    if data['geoserver'].get('db_port', '') != '':
        data['geoserver_internal_url'] += ':' + data['geoserver']['db_port']
    data['geoserver_internal_url'] += '/' + data['geoserver']['db_name'] + '/'

    core_name = data['package_name'] + "_" + data['resource_id']
    credentials = (data['geoserver']['db_user'], data['geoserver']['db_pass'])

    # Create a workspace metadata
    workspace = "ws_" + core_name
    wsurl = data['geoserver_internal_url'] + 'rest/workspaces/' + workspace

    res = requests.delete(wsurl, params={'recurse': 'true'}, auth=credentials)

    if res.status_code != 200:
        logger.info("Recursive deletion of Geoserver workspace {0} failed with error {1}".format(wsurl + '/' + workspace + '?recurse=true&quietOnNotFound', res))
    else:
        logger.info("Recursive deletion of Geoserver workspace {0} succeeded".format(wsurl + '/' + workspace + '?recurse=true&quietOnNotFound'))
