import dotenv from "dotenv";
dotenv.config();

import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { randomUUID } from "crypto";

const awsConfig = {
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY || "",
        secretAccessKey: process.env.AWS_SECRET_KEY || "",
        sessionToken: process.env.AWS_SESSION_TOKEN || "",
    },
};

const sqs = new SQSClient(awsConfig);
const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient(awsConfig));
const sns = new SNSClient(awsConfig);

async function procesarMensaje(msg: any) {
    const { message, email, url } = JSON.parse(msg.Body);

    const id = randomUUID();

    await dynamo.send(new PutCommand({
        TableName: process.env.DYNAMO_TABLE || "boletines",
        Item: {
            id,
            message,
            email,
            imageUrl: url,
            leido: false,
            createdAt: new Date().toISOString(),
        },
    }));

    const link = `${process.env.APP_URL}/boletines/${id}?correoElectronico=${encodeURIComponent(email)}`;

    await sns.send(new PublishCommand({
        TopicArn: process.env.SNS_TOPIC_ARN || "",
        Subject: "Nuevo boletín disponible",
        Message: `Se ha generado un nuevo boletín.\n\nVer contenido: ${link}`,
    }));

    await sqs.send(new DeleteMessageCommand({
        QueueUrl: process.env.SQS_URL || "",
        ReceiptHandle: msg.ReceiptHandle,
    }));

    console.log(`Boletín ${id} procesado para ${email}`);
}

async function consumir() {
    console.log("Iniciando consumo de mensajes...");
    while (true) {
        const result = await sqs.send(new ReceiveMessageCommand({
            QueueUrl: process.env.SQS_URL || "",
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 20,
        }));

        if (result.Messages && result.Messages.length > 0) {
            for (const msg of result.Messages) {
                try {
                    await procesarMensaje(msg);
                } catch (e) {
                    console.error("Error procesando mensaje:", (e as Error).message);
                }
            }
        }
    }
}

console.log("Servicio receptor listo");
consumir();
