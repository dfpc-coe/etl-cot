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
        task: ETL
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
        }, (req, res) => {
            const [scheme, token] = (req.headers.authorization || '').split(' ');

            if (scheme !== 'Bearer' || !token || token !== env.SharedSecret) {
                res.status(401).json({
                    status: 401,
                    message: 'Unauthorized'
                });
                return;
            }

            Task.receive(task, req.headers['content-type'] || '', req.body, env.DEBUG)
                .then((count) => res.json({
                    status: 200,
                    message: `Submitted ${count} Feature${count === 1 ? '' : 's'}`
                }))
                .catch((err: unknown) => {
                    console.error(err);

                    res.status(400).json({
                        status: 400,
                        message: err instanceof Error ? err.message : String(err)
                    });
                });
        });
    }

    static async receive(
        task: ETL,
        header: string,
        body: unknown,
        debug: boolean
    ): Promise<number> {
        const contentType = header.split(';')[0].trim().toLowerCase();

        let features: Static<typeof Feature.InputFeature>[];

        if (contentType === 'application/json') {
            const format = typeof body === 'object' && body !== null
                && 'type' in body && body.type === 'FeatureCollection'
                ? Feature.InputFeatureCollection
                : Feature.InputFeature;

            if (!Value.Check(format, body)) {
                const first = Value.Errors(format, body).First();
                throw new Error(`Invalid CoT GeoJSON${first ? `: ${first.path}: ${first.message}` : ''}`);
            }

            features = body.type === 'FeatureCollection'
                ? body.features
                : [body];
        } else {
            const cot = CoTParser.from_xml(String(body));
            features = [await CoTParser.to_geojson(cot)];
        }

        if (debug) {
            console.log(JSON.stringify(features, null, 2));
        }

        await task.submit({
            type: 'FeatureCollection',
            features
        });

        return features.length;
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
export async function handler(event: Event = {}, context?: object): ReturnType<typeof internal> {
    return internal(new Task(import.meta.url), event, context);
}
