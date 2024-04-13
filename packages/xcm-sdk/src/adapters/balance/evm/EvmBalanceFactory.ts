import { ContractConfig } from '@moonbeam-network/xcm-builder';
import { Erc20 } from './Erc20';
import { Native } from './Native';
import { EvmBalance } from './EvmBalance';

import { EvmClient } from '../../../evm';

export class EvmBalanceFactory {
  static get(client: EvmClient, config: ContractConfig): EvmBalance {
    switch (config.module) {
      case 'Erc20':
        return new Erc20(client, config);
      case 'Native':
        return new Native(client, config);
      default: {
        throw new Error(
          'Module type ' + config.module + ' is not supported yet'
        );
      }
    }
  }
}
