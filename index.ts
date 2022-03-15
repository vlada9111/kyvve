import Arweave from "arweave";
import { JWKInterface } from "arweave/node/lib/wallet";
import BigNumber from "bignumber.js";
import { Contract, ContractTransaction, ethers, Wallet } from "ethers";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "fs";
import Prando from "prando";
import { satisfies } from "semver";
import { ILogObject } from "tslog";
import {
  adjectives,
  colors,
  animals,
  uniqueNamesGenerator,
} from "unique-names-generator";
import { Bundle, BundleInstructions, BundleProposal } from "./faces";
import { CLI } from "./utils";
import {
  getGasPrice,
  toBN,
  toEthersBN,
  toHumanReadable,
  getPoolContract,
  getTokenContract,
  sleep,
  fromBytes,
  toBytes,
  ADDRESS_ZERO,
} from "./utils/helpers";
import { logger } from "./utils";
import { version } from "../package.json";
import Transaction from "arweave/node/lib/transaction";
import hash from "object-hash";
import http from "http";
import url from "url";
import client, { register } from "prom-client";
import { Database } from "./utils/database";
import du from "du";
import { gunzipSync, gzipSync } from "zlib";
import axios from "axios";

export * from "./utils";
export * from "./faces";
export * from "./utils/helpers";
export * from "./utils/database";
export * from "./utils/progress";

client.collectDefaultMetrics({
  labels: { app: "kyve-core" },
});

const metricsWorkerHeight = new client.Gauge({
  name: "current_worker_height",
  help: "The current height the worker has indexed to.",
});

const metricsDbSize = new client.Gauge({
  name: "current_db_size",
  help: "The size of the local database.",
});

const metricsDbUsed = new client.Gauge({
  name: "current_db_used",
  help: "The database usage in percent.",
});

class KYVE {
  protected pool: Contract;
  protected runtime: string;
  protected version: string;
  protected stake: string;
  protected commission: string;
  protected wallet: Wallet;
  protected keyfile: JWKInterface;
  protected name: string;
  protected gasMultiplier: string;
  protected poolState: any;
  protected runMetrics: boolean;
  protected space: number;
  protected db: Database;
  protected arweave = new Arweave({
    host: "arweave.net",
    protocol: "https",
  });

  public static metrics = client;

  constructor(cli?: CLI) {
    if (!cli) {
      cli = new CLI(process.env.KYVE_RUNTIME!, process.env.KYVE_VERSION!);
    }

    cli.parse();
    const options = cli.opts();

    const provider = new ethers.providers.StaticJsonRpcProvider(
      "https://rpc.api.moonbase.moonbeam.network",
      {
        chainId: 1287,
        name: "Moonbase Alpha",
      }
    );

    this.wallet = new Wallet(options.privateKey, provider);

    this.pool = getPoolContract(options.pool, this.wallet);
    this.runtime = cli.runtime;
    this.version = cli.packageVersion;
    this.stake = options.stake;
    this.commission = options.commission;
    this.keyfile =
      options.keyfile && JSON.parse(readFileSync(options.keyfile, "utf-8"));
    this.gasMultiplier = options.gasMultiplier;
    this.runMetrics = options.metrics;
    this.space = +options.space;
    this.name = options?.name ?? this.generateRandomName();

    this.db = new Database(this.name);

    if (!existsSync("./logs")) {
      mkdirSync("./logs");
    }

    const logToTransport = (log: ILogObject) => {
      appendFileSync(`./logs/${this.name}.txt`, JSON.stringify(log) + "\n");
    };

    logger.setSettings({
      minLevel: options.verbose ? undefined : "info",
    });

    logger.attachTransport({
      silly: logToTransport,
      debug: logToTransport,
      trace: logToTransport,
      info: logToTransport,
      warn: logToTransport,
      error: logToTransport,
      fatal: logToTransport,
    });
  }

  async start() {
    this.logNodeInfo();
    this.setupMetrics();

    try {
      await this.fetchPoolState();
    } catch {
      process.exit(1);
    }

    await this.setupNodeStake();
    await this.setupNodeCommission();

    try {
      await this.checkIfNodeIsValidator();
    } catch {
      process.exit(1);
    }

    this.worker();
    this.run();
  }

  private async run() {
    try {
      while (true) {
        console.log("");
        logger.info("⚡️ Starting new proposal");

        let bundleProposal;
        let bundleInstructions;

        try {
          await this.fetchPoolState(false);
        } catch {
          await sleep(60 * 1000);
          continue;
        }

        if (this.poolState.paused) {
          logger.info("💤  Pool is paused. Waiting ...");
          await sleep(60 * 1000);
          continue;
        }

        try {
          await this.checkIfNodeIsValidator(false);
        } catch {
          await sleep(60 * 1000);
          continue;
        }

        await this.clearFinalizedData();

        bundleProposal = await this.getBundleProposal();
        bundleInstructions = await this.getBundleInstructions();

        if (bundleInstructions.uploader === this.wallet.address) {
          logger.info("📚 Selected as UPLOADER");
        } else {
          logger.info("🧐 Selected as VALIDATOR");
        }

        if (
          bundleProposal.uploader !== ADDRESS_ZERO &&
          bundleProposal.uploader !== this.wallet.address
        ) {
          if (await this.pool.canVote()) {
            await this.validateProposal(bundleProposal);

            bundleProposal = await this.getBundleProposal();
            bundleInstructions = await this.getBundleInstructions();
          } else {
            logger.debug("Can not vote this round. Skipping ...");
          }
        }

        if (bundleInstructions.uploader === ADDRESS_ZERO) {
          await this.claimUploaderRole();

          bundleProposal = await this.getBundleProposal();
          bundleInstructions = await this.getBundleInstructions();
        }

        if (bundleInstructions.uploader === this.wallet.address) {
          logger.debug("Waiting for proposal quorum ...");
        }

        while (true) {
          bundleProposal = await this.getBundleProposal();
          bundleInstructions = await this.getBundleInstructions();

          if (bundleInstructions.uploader === this.wallet.address) {
            if (await this.pool.canPropose()) {
              // if upload fails try again & refetch bundleInstructions
              await this.uploadBundleToArweave(
                bundleProposal,
                bundleInstructions
              );
              break;
            } else {
              await sleep(10 * 1000);
            }
          } else {
            break;
          }
        }

        await this.nextBundleInstructions(bundleInstructions);
      }
    } catch (error) {
      logger.error(`❌ Runtime error. Exiting ...`);
      logger.debug(error);
      process.exit(1);
    }
  }

  public async worker() {
    while (true) {
      try {
        let workerHeight;

        try {
          workerHeight = parseInt(await this.db.get("head"));
        } catch {
          workerHeight = this.poolState.height.toNumber();
        }

        const usedDiskSpace = await du(`./db/${this.name}/`);
        const usedDiskSpacePercent = parseFloat(
          ((usedDiskSpace * 100) / this.space).toFixed(2)
        );

        metricsWorkerHeight.set(workerHeight);
        metricsDbSize.set(usedDiskSpace);
        metricsDbUsed.set(usedDiskSpacePercent);

        if (usedDiskSpace > this.space) {
          logger.debug(`Used disk space: ${usedDiskSpacePercent}%`);
          await sleep(60 * 1000);
          continue;
        }

        const ops = await this.requestWorkerBatch(workerHeight);

        for (let op of ops) {
          await this.db.put(op.key, op.value);
        }

        await this.db.put("head", workerHeight + ops.length);
      } catch (error) {
        logger.error("❌ Error requesting data batch.");
        logger.debug(error);
        await sleep(10 * 1000);
      }
    }
  }

  public async requestWorkerBatch(workerHeight: number): Promise<any[]> {
    logger.error(`❌ "requestWorkerBatch" not implemented. Exiting ...`);
    process.exit(1);
  }

  public async createBundle(
    bundleInstructions: BundleInstructions
  ): Promise<Bundle> {
    logger.error(`❌ "createBundle" not implemented. Exiting ...`);
    process.exit(1);
  }

  public async loadBundle(bundleProposal: BundleProposal): Promise<Buffer> {
    logger.error(`❌ "loadBundle" not implemented. Exiting ...`);
    process.exit(1);
  }

  private async clearFinalizedData() {
    let tail: number;

    try {
      tail = parseInt(await this.db.get("tail"));
    } catch {
      tail = this.poolState.height.toNumber();
    }

    for (let key = tail; key < this.poolState.height.toNumber(); key++) {
      try {
        await this.db.del(key);
      } catch {}
    }

    await this.db.put("tail", this.poolState.height.toNumber());
  }

  private async validateProposal(bundleProposal: BundleProposal) {
    logger.info(`🔬 Validating bundle ${bundleProposal.txId}`);
    logger.debug(`Downloading bundle from Arweave ...`);

    let uploadBundle;
    let downloadBundle;

    let tries = 0;

    while (tries < 10) {
      downloadBundle = await this.downloadBundleFromArweave(bundleProposal);

      if (downloadBundle) {
        logger.debug(
          `Loading local bundle from ${bundleProposal.fromHeight} to ${bundleProposal.toHeight} ...`
        );

        uploadBundle = gzipSync(await this.loadBundle(bundleProposal));

        await this.vote({
          transaction: bundleProposal.txId,
          valid: await this.validate(
            uploadBundle,
            +bundleProposal.byteSize,
            downloadBundle,
            +downloadBundle.byteLength
          ),
        });
        break;
      } else {
        logger.error(
          `❌ Error fetching bundle from Arweave. Retrying in 30s ...`
        );
        await sleep(30 * 1000);

        tries++;

        break;
      }
    }
  }

  public async validate(
    uploadBundle: Buffer,
    uploadBytes: number,
    downloadBundle: Buffer,
    downloadBytes: number
  ): Promise<boolean> {
    if (uploadBytes !== downloadBytes) {
      return false;
    }

    if (hash(uploadBundle) !== hash(downloadBundle)) {
      return false;
    }

    return true;
  }

  private async getBundleProposal(): Promise<BundleProposal> {
    const proposal = {
      ...(await this.pool.bundleProposal()),
    };

    return {
      uploader: proposal.uploader,
      txId: fromBytes(proposal.txId),
      parentTxId: fromBytes(proposal.parentTxId),
      byteSize: proposal.byteSize.toNumber(),
      fromHeight: proposal.fromHeight.toNumber(),
      toHeight: proposal.toHeight.toNumber(),
      start: proposal.start.toNumber(),
    };
  }

  private async getBundleInstructions(): Promise<BundleInstructions> {
    const instructions = {
      ...(await this.pool.bundleInstructions()),
    };

    return {
      uploader: instructions.uploader,
      fromHeight: instructions.fromHeight.toNumber(),
    };
  }

  private async downloadBundleFromArweave(
    bundleProposal: BundleProposal
  ): Promise<any> {
    try {
      const { status } = await this.arweave.transactions.getStatus(
        bundleProposal.txId
      );

      if (status === 200 || status === 202) {
        const { data: downloadBundle } = await axios.get(
          `https://arweave.net/${bundleProposal.txId}`,
          { responseType: "arraybuffer" }
        );

        return downloadBundle;
      }
      return null;
    } catch {
      return null;
    }
  }

  private async uploadBundleToArweave(
    bundleProposal: BundleProposal,
    bundleInstructions: BundleInstructions
  ): Promise<void> {
    try {
      logger.info("📦 Creating new bundle proposal");

      const uploadBundle = await this.createBundle(bundleInstructions);

      logger.debug("Uploading bundle to Arweave ...");

      const transaction = await this.arweave.createTransaction({
        data: gzipSync(uploadBundle.bundle),
      });

      logger.debug(
        `Bundle details = bytes: ${transaction.data_size}, items: ${
          uploadBundle.toHeight - uploadBundle.fromHeight
        }`
      );

      transaction.addTag("Application", "KYVE - Testnet");
      transaction.addTag("Pool", this.pool.address);
      transaction.addTag("@kyve/core", version);
      transaction.addTag(this.runtime, this.version);
      transaction.addTag("Uploader", bundleInstructions.uploader);
      transaction.addTag("FromHeight", uploadBundle.fromHeight.toString());
      transaction.addTag("ToHeight", uploadBundle.toHeight.toString());
      transaction.addTag("Content-Type", "application/gzip");

      if (bundleProposal.uploader === ADDRESS_ZERO) {
        transaction.addTag("Parent", bundleProposal.parentTxId);
      } else {
        transaction.addTag("Parent", bundleProposal.txId);
      }

      await this.arweave.transactions.sign(transaction, this.keyfile);

      const balance = await this.arweave.wallets.getBalance(
        await this.arweave.wallets.getAddress(this.keyfile)
      );

      if (+transaction.reward > +balance) {
        logger.error("❌ You do not have enough funds in your Arweave wallet.");
        process.exit(1);
      }

      await this.arweave.transactions.post(transaction);

      const tx = await this.pool.submitBundleProposal(
        toBytes(transaction.id),
        +transaction.data_size,
        uploadBundle.toHeight - uploadBundle.fromHeight,
        {
          gasLimit: ethers.BigNumber.from(1000000),
          gasPrice: await getGasPrice(this.pool, this.gasMultiplier),
        }
      );

      logger.debug(`Arweave Transaction ${transaction.id} ...`);
      logger.debug(`Transaction = ${tx.hash}`);
    } catch (error) {
      logger.error(
        "❌ Received an error while trying to upload bundle to Arweave. Skipping upload ..."
      );
      logger.debug(error);
    }
  }

  private async claimUploaderRole() {
    try {
      logger.info("🔍 Claiming uploader role ...");

      const tx = await this.pool.claimUploaderRole({
        gasLimit: ethers.BigNumber.from(1000000),
        gasPrice: await getGasPrice(this.pool, this.gasMultiplier),
      });
      logger.debug(`Transaction = ${tx.hash}`);

      await tx.wait();
    } catch (error) {
      logger.error(
        "❌ Received an error while to claim uploader role. Skipping ..."
      );
      logger.debug(error);
    }
  }

  private async nextBundleInstructions(
    bundleInstructions: BundleInstructions
  ): Promise<void> {
    return new Promise((resolve) => {
      logger.debug("Waiting for new proposal ...");

      const uploadTimeout = setInterval(async () => {
        try {
          if (bundleInstructions.uploader !== this.wallet.address) {
            if (await this.pool.canClaim()) {
              await this.claimUploaderRole();
            }
          }
        } catch (error) {
          logger.error(
            "❌ Received an error while claiming uploader role. Skipping claim ..."
          );
          logger.debug(error);
        }
      }, 10 * 1000);

      this.pool.on("NextBundleInstructions", () => {
        clearInterval(uploadTimeout);
        resolve();
      });
    });
  }

  private async vote(vote: { transaction: string; valid: boolean }) {
    logger.info(
      `🖋  Voting ${vote.valid ? "valid" : "invalid"} on bundle ${
        vote.transaction
      } ...`
    );

    try {
      const tx = await this.pool.vote(toBytes(vote.transaction), vote.valid, {
        gasLimit: await this.pool.estimateGas.vote(
          toBytes(vote.transaction),
          vote.valid
        ),
        gasPrice: await getGasPrice(this.pool, this.gasMultiplier),
      });
      logger.debug(`Transaction = ${tx.hash}`);
    } catch (error) {
      logger.error("❌ Received an error while trying to vote. Skipping ...");
      logger.debug(error);
    }
  }

  private logNodeInfo() {
    const formatInfoLogs = (input: string) => {
      const length = Math.max(13, this.runtime.length);
      return input.padEnd(length, " ");
    };

    logger.info(
      `🚀 Starting node ...\n\t${formatInfoLogs("Name")} = ${
        this.name
      }\n\t${formatInfoLogs("Address")} = ${
        this.wallet.address
      }\n\t${formatInfoLogs("Pool")} = ${this.pool.address}\n\t${formatInfoLogs(
        "Desired Stake"
      )} = ${this.stake} $KYVE\n\n\t${formatInfoLogs(
        "@kyve/core"
      )} = v${version}\n\t${formatInfoLogs(this.runtime)} = v${this.version}`
    );
  }

  private setupMetrics() {
    if (this.runMetrics) {
      logger.info(
        "🔬 Starting metric server on: http://localhost:8080/metrics"
      );

      // HTTP server which exposes the metrics on http://localhost:8080/metrics
      http
        .createServer(async (req: any, res: any) => {
          // Retrieve route from request object
          const route = url.parse(req.url).pathname;

          if (route === "/metrics") {
            // Return all metrics the Prometheus exposition format
            res.setHeader("Content-Type", register.contentType);
            const defaultMetrics = await register.metrics();
            const other = await KYVE.metrics.register.metrics();
            res.end(defaultMetrics + "\n" + other);
          }
        })
        .listen(8080);
    }
  }

  private async fetchPoolState(logs: boolean = true) {
    if (logs) {
      logger.debug("Attempting to fetch pool state.");
    }

    try {
      this.poolState = { ...(await this.pool.pool()) };
    } catch (error) {
      logger.error(
        "❌ Received an error while trying to fetch the pool state:"
      );
      logger.debug(error);
      throw new Error();
    }

    try {
      this.poolState.config = JSON.parse(this.poolState.config);
    } catch (error) {
      logger.error("❌ Received an error while trying to parse the config:");
      logger.debug(error);
      throw new Error();
    }

    try {
      this.poolState.metadata = JSON.parse(this.poolState.metadata);
    } catch (error) {
      logger.error("❌ Received an error while trying to parse the metadata:");
      logger.debug(error);
      throw new Error();
    }

    if (this.poolState.metadata?.runtime === this.runtime) {
      if (logs) {
        logger.info(`💻 Running node on runtime ${this.runtime}.`);
      }
    } else {
      logger.error("❌ Specified pool does not match the integration runtime.");
      process.exit(1);
    }

    try {
      if (
        satisfies(
          this.version,
          this.poolState.metadata?.versions || this.version
        )
      ) {
        if (logs) {
          logger.info("⏱  Pool version requirements met.");
        }
      } else {
        logger.error(
          `❌ Running an invalid version for the specified pool. Version requirements are ${this.poolState.metadata.versions}.`
        );
        process.exit(1);
      }
    } catch (error) {
      logger.error("❌ Received an error while trying parse versions");
      logger.debug(error);
      process.exit(1);
    }

    if (logs) {
      logger.info("✅ Fetched pool state.");
    }
  }

  private async checkIfNodeIsValidator(logs: boolean = true) {
    try {
      const isValidator = await this.pool.isValidator(this.wallet.address);

      if (isValidator) {
        if (logs) {
          logger.info("🔍  Node is running as a validator.");
        }
      } else {
        logger.error("❌ Node is no active validator. Exiting ...");
        process.exit(1);
      }
    } catch (error) {
      logger.error("❌ Received an error while trying to fetch validator info");
      logger.debug(error);
      throw new Error();
    }
  }

  private async setupNodeStake() {
    let parsedStake;

    logger.info("🌐 Joining KYVE Network ...");

    let nodeStake = toBN(
      (await this.pool.node(this.wallet.address)).personalStake
    );

    try {
      parsedStake = new BigNumber(this.stake).multipliedBy(
        new BigNumber(10).exponentiatedBy(18)
      );

      if (parsedStake.isZero()) {
        logger.error("❌ Desired stake can't be zero.");
        process.exit(1);
      }
    } catch (error) {
      logger.error("❌ Provided invalid staking amount:", error);
      process.exit(1);
    }

    if (parsedStake.lt(toBN(this.poolState.minStake))) {
      logger.error(
        `❌ Desired stake is lower than the minimum stake. Desired Stake = ${toHumanReadable(
          parsedStake
        )}, Minimum Stake = ${toHumanReadable(toBN(this.poolState.minStake))}`
      );
      process.exit();
    }

    if (parsedStake.gt(nodeStake)) {
      // Stake the difference.
      const diff = parsedStake.minus(nodeStake);
      await this.selfStake(diff);
    } else if (parsedStake.lt(nodeStake)) {
      // Unstake the difference.
      const diff = nodeStake.minus(parsedStake);
      await this.selfUnstake(diff);
    } else {
      logger.info("👌 Already staked with the correct amount.");
    }
  }

  private async selfStake(amount: BigNumber) {
    const token = await getTokenContract(this.pool);
    let tx: ContractTransaction;

    const balance = toBN(
      (await token.balanceOf(this.wallet.address)) as ethers.BigNumber
    );

    if (balance.lt(amount)) {
      logger.error("❌ Supplied wallet does not have enough $KYVE to stake.");
      process.exit(1);
    }

    try {
      tx = await token.approve(this.pool.address, toEthersBN(amount), {
        gasLimit: await token.estimateGas.approve(
          this.pool.address,
          toEthersBN(amount)
        ),
        gasPrice: await getGasPrice(this.pool, this.gasMultiplier),
      });
      logger.debug(
        `Approving ${toHumanReadable(
          amount
        )} $KYVE to be spent. Transaction = ${tx.hash}`
      );

      await tx.wait();
      logger.info("👍 Successfully approved.");

      tx = await this.pool.stake(toEthersBN(amount), {
        gasLimit: await this.pool.estimateGas.stake(toEthersBN(amount)),
        gasPrice: await getGasPrice(this.pool, this.gasMultiplier),
      });
      logger.debug(
        `Staking ${toHumanReadable(amount)} $KYVE. Transaction = ${tx.hash}`
      );

      await tx.wait();
      logger.info("📈 Successfully staked.");
    } catch (error) {
      logger.error("❌ Received an error while trying to stake:", error);
      process.exit(1);
    }
  }

  private async selfUnstake(amount: BigNumber) {
    let tx: ContractTransaction;

    try {
      tx = await this.pool.unstake(toEthersBN(amount), {
        gasLimit: await this.pool.estimateGas.unstake(toEthersBN(amount)),
        gasPrice: await getGasPrice(this.pool, this.gasMultiplier),
      });
      logger.debug(`Unstaking. Transaction = ${tx.hash}`);

      await tx.wait();
      logger.info("📉 Successfully unstaked.");
    } catch (error) {
      logger.error("❌ Received an error while trying to unstake:", error);
      process.exit(1);
    }
  }

  private async setupNodeCommission() {
    let parsedCommission;

    logger.info("👥 Setting node commission ...");

    let nodeCommission = toBN(
      (await this.pool.node(this.wallet.address)).commission
    );

    try {
      parsedCommission = new BigNumber(this.commission).multipliedBy(
        new BigNumber(10).exponentiatedBy(18)
      );

      if (parsedCommission.lt(0) && parsedCommission.gt(100)) {
        logger.error("❌ Desired commission must be between 0 and 100.");
        process.exit(1);
      }
    } catch (error) {
      logger.error("❌ Provided invalid commission amount:", error);
      process.exit(1);
    }

    if (!parsedCommission.eq(nodeCommission)) {
      try {
        const tx = await this.pool.updateCommission(
          toEthersBN(parsedCommission),
          {
            gasLimit: await this.pool.estimateGas.updateCommission(
              toEthersBN(parsedCommission)
            ),
            gasPrice: await getGasPrice(this.pool, this.gasMultiplier),
          }
        );
        logger.debug(`Updating commission. Transaction = ${tx.hash}`);

        await tx.wait();
        logger.info("💼 Successfully updated commission.");
      } catch (error) {
        logger.error(
          "❌ Received an error while trying to update commission:",
          error
        );
        process.exit(1);
      }
    } else {
      logger.info("👌 Already set correct commission.");
    }
  }

  // TODO: move to separate file
  private generateRandomName() {
    const r = new Prando(this.wallet.address + this.pool.address);

    return uniqueNamesGenerator({
      dictionaries: [adjectives, colors, animals],
      separator: "-",
      length: 3,
      style: "lowerCase",
      seed: r.nextInt(0, adjectives.length * colors.length * animals.length),
    }).replace(" ", "-");
  }
}

export default KYVE;
