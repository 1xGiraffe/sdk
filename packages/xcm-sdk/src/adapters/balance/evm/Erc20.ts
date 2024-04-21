import { ContractConfig } from '@moonbeam-network/xcm-builder';

import { EvmBalance } from './EvmBalance';
import { EvmClient, Erc20Client } from '../../../evm';

export class Erc20 extends EvmBalance {
  readonly erc20: Erc20Client;

  constructor(client: EvmClient, config: ContractConfig) {
    super(client, config);
    this.erc20 = new Erc20Client(client, this.address);
  }

  get address() {
    return this.config.address!;
  }

  async getBalance(): Promise<bigint> {
    const { args } = this.config;
    const [account] = args;
    return this.erc20.balanceOf(account);
  }

  async getDecimals(): Promise<number> {
    return this.erc20.decimals();
  }
}