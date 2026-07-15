import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import type { Event } from '@tak-ps/etl';
import { CoTParser, Feature } from '@tak-ps/node-cot';
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import type Schema from '@openaddresses/batch-schema';

const InputSchema = Type.Object({
    DEBUG: Type.Boolean({
        default: false,
        description: 'Print received CoT features in logs'
    })
});

const OutputSchema = Type.Object({
    metadata: Type.Optional(Type.Record(Type.String(), Type.Unknown()))
});

/**
 * Validate an unknown input against a TypeBox schema, throwing a
 * descriptive error suitable for a 400 response on failure
 */
function validate<T extends TSchema>(schema: T, input: unknown): Static<T> {
    if (Value.Check(schema, input)) return input;

    const first = Value.Errors(schema, input).First();
    throw new Error(`Invalid CoT GeoJSON${first ? `: ${first.path}: ${first.message}` : ''}`);
}

/**
 * Accept either a single node-cot InputFeature or an InputFeatureCollection
 */
function parseJSON(body: unknown): Static<typeof Feature.InputFeature>[] {
    if (
        body && typeof body === 'object'
        && (body as { type?: unknown }).type === 'FeatureCollection'
    ) {
        return validate(Feature.InputFeatureCollection, body).features;
    }

    return [validate(Feature.InputFeature, body)];
}

/**
 * Parse a raw CoT XML Event document and convert it to a GeoJSON Feature
 */
async function parseXML(xml: string): Promise<Static<typeof Feature.InputFeature>> {
    const cot = CoTParser.from_xml(xml);
    return await CoTParser.to_geojson(cot);
}

export default class Task extends ETL {
    static name = 'etl-cot';
    static flow = [DataFlowType.Incoming];
    static invocation = [InvocationType.Webhook];
    static invocationDefaults = {
        webhook: {
            enabled: true
        }
    };

    static async webhooks(
        schema: Schema,
        task: Task
    ): Promise<void> {
        const env = await task.env(InputSchema);

        await schema.post('/:webhookid', {
            name: 'Incoming Webhook',
            group: 'Default',
            description: 'Endpoint for receiving CoT events as node-cot GeoJSON (application/json) or raw CoT XML (application/xml, text/xml)',
            params: Type.Object({
                webhookid: Type.String({
                    description: 'Unique identifier for the webhook'
                })
            }),
            body: {
                'application/json': true,
                'application/xml': true,
                'text/xml': true
            },
            res: Type.Object({
                status: Type.Integer(),
                message: Type.String()
            })
        }, async (req, res) => {
            try {
                const contentType = String(req.headers['content-type'] || '')
                    .split(';')[0].trim().toLowerCase();

                const features = contentType === 'application/json'
                    ? parseJSON(req.body)
                    : [await parseXML(String(req.body))];

                if (env.DEBUG) {
                    console.log(JSON.stringify(features, null, 2));
                }

                await task.submit({
                    type: 'FeatureCollection',
                    features
                });

                return res.json({
                    status: 200,
                    message: `Submitted ${features.length} Feature${features.length === 1 ? '' : 's'}`
                });
            } catch (err) {
                console.error(err);

                return res.status(400).json({
                    status: 400,
                    message: err instanceof Error ? err.message : String(err)
                });
            }
        });
    }

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            return type === SchemaType.Input ? InputSchema : OutputSchema;
        }

        return Type.Object({});
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}, context?: object) {
    return await internal(new Task(import.meta.url), event, context);
}
