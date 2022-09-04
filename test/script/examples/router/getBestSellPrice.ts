import { ApiPromise } from '@polkadot/api';
import { PolkadotExecutor } from '../../executor';
import { PolkadotPoolService } from '../../../../src/pool';
import { TradeRouter } from '../../../../src/api';
import { bnum, scale } from '../../../../src/utils/bignumber';

class GetBestSellPriceExample extends PolkadotExecutor {
  async script(api: ApiPromise): Promise<any> {
    const poolService = new PolkadotPoolService(api);
    const router = new TradeRouter(poolService);
    return router.getBestSellPrice('1', '2', scale(bnum('1'), 12));
  }
}

new GetBestSellPriceExample(
  'wss://rpc.basilisk.cloud',
  'Get best sell price',
  true
).run();