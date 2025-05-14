import { AaveUtils, bnum } from '../../../../src';

const BENEFICIARY = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY';

const main = async () => {
  const aave = new AaveUtils();
  const result = await aave.getHealthFactorAfterSupply(
    BENEFICIARY,
    '15',
    bnum('160000000000')
  );
  console.log(result);
};

main()
  .then(() => console.log('Get HF after supply complete ✅'))
  .catch(console.error)
  .finally(() => process.exit(0));
