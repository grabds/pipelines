import { Client, ClientOptions, ResultCallback } from "minio";
import fetch from "node-fetch";
import { Stream } from "stream";

declare module "minio" {
  /**
   * NOTE: minio type-def for ClientOptions is outdated (v6 instead of v7).
   * `secure` is replaced by `useSSL`.
   */

  export interface ClientOptions {
    endPoint: string;
    accessKey: string;
    secretKey: string;
    useSSL?: boolean;
    port?: number;
    region?: Region;
    transport?: any;
    sessionToken?: string;
  }
}

/**
 * Expected environment variables to configure minio client.
 */
export interface IMinioEnv extends NodeJS.ProcessEnv {
  MINIO_END_POINT?: string;
  MINIO_PORT?: string;
  MINIO_ACCESS_KEY?: string;
  MINIO_SECRET_KEY?: string;
  MINIO_SECURE?: string;
  MINIO_REGION?: string;
}

/**
 * Expected output (partial) from role iam credential request to EC2 metadata store.
 */
type MetaDataResult = {
  AccessKeyId: string;
  SecretAccessKey: string;
  Expiration: string;
  Token: string;
};

/**
 * Whether is an amazon end point.
 * @param endpoint endpoint
 */
const isAmazonEndpoint = (endpoint: string) =>
  endpoint === "s3.amazonaws.com" ||
  endpoint === "s3.cn-north-1.amazonaws.com.cn";

/**
 * Create a minio ClientOptions from the environment variables.
 * @param env Environment variables
 */
export const confFromEnv = (env: IMinioEnv = process.env): ClientOptions => {
  const {
    MINIO_END_POINT: endPoint = "minio-service.kubeflow",
    MINIO_PORT: port = "9000",
    MINIO_ACCESS_KEY: accessKey = "minio",
    MINIO_SECRET_KEY: secretKey = "minio123",
    MINIO_SECURE: useSSL = "false",
    MINIO_REGION: region
  } = env;

  const asBool = (value?: string) =>
    !!value ? ["true", "1"].indexOf(value.toLowerCase()) >= 0 : false;

  return {
    endPoint,
    port: parseInt(port, 10),
    useSSL: asBool(useSSL),
    accessKey,
    secretKey,
    region
  };
};

export class MinioClientManager {
  /**
   * AWS EC2 metadata store.
   */
  public readonly metadataUrl: string =
    "http://169.254.169.254/latest/meta-data";
  /**
   * Promise to EC2 instance profile if available.
   */
  public iamRole!: Promise<string | void>;

  /**
   * promise to minio client
   */
  private _client?: Promise<Client | void>;

  /**
   * expiration time (epoch time in ms) for IAM role credentials.
   * If access credentials are provided, expiration time will be
   * -1.
   */
  protected expiration: number = 0;

  /**
   * Class that returns a minio client based on environment
   * variables. Fallback to EC2 IAM role credentials if
   * access key and secret are empty strings.
   */
  constructor(protected options: ClientOptions) {
    this.init();
  }

  /**
   * @returns Promise to a minio client.
   */
  client() {
    // create new minio client if required
    this._client = this.createClient();
    return this._client;
  }

  /**
   * Initialize a minio client if credentials are provided.
   */
  protected init() {
    const { endPoint, accessKey = "", secretKey = "" } = this.options;

    if (isAmazonEndpoint(endPoint.toLowerCase())) {
      // no credential provided, fallback to iam-role credentials
      if (accessKey.length === 0 || secretKey.length === 0) return;
      // never expire
      this.expiration = -1;
      // create normal persistent client
      this._client = Promise.resolve(new Client(this.options));
      return;
    }

    // never expire
    this.expiration = -1;
    // create normal persistent client
    this._client = Promise.resolve(new Client(this.options));
  }

  protected getClientFromEc2MetaData(iamRole?: string) {
    if (!iamRole) throw new Error("Unable to find IAM role for EC2 instance.");

    // create minio Client from iam-role credentials
    const toClient = ({
      AccessKeyId: accessKey,
      SecretAccessKey: secretKey,
      Expiration,
      Token: sessionToken
    }: MetaDataResult) => {
      // update ms to expiration
      this.expiration = new Date(Expiration).getTime();
      return new Client({
        endPoint: "s3.amazonaws.com",
        accessKey,
        secretKey,
        sessionToken
      });
    };
    return fetch(`${this.metadataUrl}/iam/security-credentials/${iamRole}`)
      .then(resp => resp.json())
      .then(toClient);
  }

  protected getClient() {
  }

  /**
   * Retrieve ec2 iam role credentials and create a new minio Client
   */
  protected createClient() {
    // return persistent client
    if (this.expiration < 0) return this._client;

    // return client if it exists and credentials are not expired.
    if (!!this._client && this.expiration > Date.now()) {
      return this._client;
    }
    // promise to iam-role for ec2 instance
    this.iamRole =
      this.iamRole ||
      fetch(`${this.metadataUrl}/iam/security-credentials/`, {
        timeout: 500
      }).then(resp => resp.ok && resp.text());
    
      // create new client with IAM credentials
    this._client = this.iamRole.then(iamRole => iamRole && this.getClientFromEc2MetaData(iamRole));

    return this._client;
  }

  /**
   * Proxy for minio getObject function.
   * @param bucket Bucket name
   * @param key Object to retrieve
   * @param cb Callback function
   */
  getObject(bucket: string, key: string, cb: ResultCallback<Stream>) {
    return this.client().then(
      client => client && client.getObject(bucket, key, cb)
    );
  }
}

/**
 * returns a `Proxy` object that proxy to a minio client. 
 * `getter` to values are returned as promises to the values,
 * while `getter` to functions will return decorated functions
 * that return a promise to the function's return.
 * @param opts minio client options
 * 
 * @example
  ```ts
  const clientProxy = getMinioClientManager(opts);

  // a promise to a stream is returned instead of a stream
  const stream = await client.listObjects("foo-bucket", "prefix");
  stream.on("data", data => console.log(data))
  
  ```
 */
export function getMinioClientProxy(opts: ClientOptions) {
  const manager = new MinioClientManager(opts);
  const isAFunction = (client: Client, key: string | symbol | number) =>
    typeof client[key] === "function";

  return new Proxy(new Client(opts), {
    get: (oTarget, sKey) =>
      isAFunction(oTarget, sKey)
        ? (...args: any[]) =>
            manager.client().then(client => client[sKey].apply(client, args))
        : manager.client().then(client => client[sKey])
  });
}
