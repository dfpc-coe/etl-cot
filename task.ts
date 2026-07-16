import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import type { Event } from '@tak-ps/etl';
import { CoTParser, Feature } from '@tak-ps/node-cot';
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import type Schema from '@openaddresses/batch-schema';

const InputSchema = Type.Object({
    SharedSecret: Type.String({
        description: 'Shared secret that callers must provide in the Authorization Bearer header'
    }),
    DEBUG: Type.Boolean({
        default: false,
        description: 'Print received CoT features in logs'
    })
});

const OutputSchema = Type.Object({
    metadata: Type.Optional(Type.Record(Type.String(), Type.Unknown()))
});

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
                const [scheme, token] = String(req.headers.authorization || '').split(' ');

                if (scheme !== 'Bearer' || !token || token !== env.SharedSecret) {
                    return res.status(401).json({
                        status: 401,
                        message: 'Unauthorized'
                    });
                }

                const contentType = String(req.headers['content-type'] || '')
                    .split(';')[0].trim().toLowerCase();

                let features: Static<typeof Feature.InputFeature>[];

                if (contentType === 'application/json') {
                    const schema = req.body && typeof req.body === 'object'
                        && (req.body as { type?: unknown }).type === 'FeatureCollection'
                        ? Feature.InputFeatureCollection
                        : Feature.InputFeature;

                    if (!Value.Check(schema, req.body)) {
                        const first = Value.Errors(schema, req.body).First();
                        throw new Error(`Invalid CoT GeoJSON${first ? `: ${first.path}: ${first.message}` : ''}`);
                    }

                    features = req.body.type === 'FeatureCollection'
                        ? req.body.features
                        : [req.body];
                } else {
                    const cot = CoTParser.from_xml(String(req.body));
                    features = [await CoTParser.to_geojson(cot)];
                }

                if (env.DEBUG) {
                    console.log(JSON.stringify(features, null, 2));
                }

                const count = features.length;

                await task.submit({
                    type: 'FeatureCollection',
                    features
                });

                return res.json({
                    status: 200,
                    message: `Submitted ${count} Feature${count === 1 ? '' : 's'}`
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
