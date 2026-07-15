<h1 align='center'>ETL-CoT</h1>

<p align='center'>Webhook ETL for receiving Cursor-On-Target events as GeoJSON or XML</p>

## Overview

Exposes a CloudTAK webhook endpoint that accepts Cursor-On-Target events in either of two formats,
determined by the request `Content-Type` header:

| Content-Type                    | Payload |
| ------------------------------- | ------- |
| `application/json`              | A [node-cot](https://github.com/dfpc-coe/node-CoT/) `InputFeature` or `InputFeatureCollection` |
| `application/xml` or `text/xml` | A raw CoT XML `<event/>` document |

XML payloads are parsed and converted to GeoJSON via node-cot before submission.

### Examples

Submit a GeoJSON Feature:

```sh
curl -X POST "${WEBHOOK_URL}" \
    -H 'Content-Type: application/json' \
    -d '{
        "id": "UNIT-123",
        "type": "Feature",
        "properties": { "type": "a-f-G", "how": "m-g", "callsign": "Unit123" },
        "geometry": { "type": "Point", "coordinates": [-105.0, 39.0] }
    }'
```

Submit a CoT XML Event:

```sh
curl -X POST "${WEBHOOK_URL}" \
    -H 'Content-Type: application/xml' \
    -d '<event version="2.0" uid="UNIT-123" type="a-f-G" how="m-g"
        time="2026-07-15T12:00:00.000Z" start="2026-07-15T12:00:00.000Z" stale="2026-07-15T12:05:00.000Z">
        <point lat="39.0" lon="-105.0" hae="1600.0" ce="10.0" le="10.0"/>
        <detail><contact callsign="Unit123"/></detail>
    </event>'
```

## Development

DFPC provided Lambda ETLs are currently all written in [NodeJS](https://nodejs.org/en) through the use of a AWS Lambda optimized
Docker container. Documentation for the Dockerfile can be found in the [AWS Help Center](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html)

```sh
npm install
```

Add a .env file in the root directory that gives the ETL script the necessary variables to communicate with a local ETL server.
When the ETL is deployed the `ETL_API` and `ETL_LAYER` variables will be provided by the Lambda Environment

```json
{
    "ETL_API": "http://localhost:5001",
    "ETL_LAYER": "19"
}
```

To run the task, ensure the local [CloudTAK](https://github.com/dfpc-coe/CloudTAK/) server is running and then run the
webhook server with the typescript runtime or build to JS and run natively with node

```
npx tsx task.ts control:webhooks
```

```
npm run build
cp .env dist/
node dist/task.js control:webhooks
```

### Deployment

Deployment into the CloudTAK environment for configuration is done via automatic releases to the DFPC AWS environment.

Github actions will build and push docker releases on every version tag which can then be automatically configured via the 
CloudTAK API.

Non-DFPC users will need to setup their own docker => ECS build system via something like Github Actions or AWS Codebuild.
