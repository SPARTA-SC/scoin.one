local utils = import 'utils.jsonnet';
local repo = 'tezos_index';
[{
  kind: 'pipeline',
  name: repo,
  trigger: utils.default_trigger,
  volumes: utils.volumes(repo),
  steps: [
    utils.golang('build',
                 ['go build .']),
  ] + utils.default_publish(repo) + [
    //deploy to develop
    utils.deploy('deploy-develop',
                 'dev',
                 'biz',
                 repo,
                 utils.adjust_deployment(['tezos-polling',
                 'tezos-api',
                 'tezos-notifier'
                 ], 'dev'),
                 { branch: ['feature/*','hotfix/*', 'develop'], event: 'push' }),

    //deploy to staging
    utils.deploy('deploy-staging',
                 'staging',
                 'biz',
                 repo,
                 utils.adjust_deployment(['tezos-polling',
                 'tezos-api',
                 'tezos-notifier'
                 ], 'staging'),
                 { branch: ['release/*'], event: 'push' })
    ,
    utils.default_slack,
  ],
}] + utils.default_secrets
