import boto3
import json
from kubernetes import client, config

def get_aws_rds_instances(clusters):
    # AWS RDS 클라이언트 생성
    rds = boto3.client('rds')

    # 인스턴스 정보를 저장할 리스트 초기화
    instance_info = []

    # 각 클러스터에 대해
    for cluster_id in clusters:
        # 클러스터에 속한 인스턴스 가져오기
        instances = rds.describe_db_instances(
            Filters=[
                {
                    'Name': 'db-cluster-id',
                    'Values': [cluster_id]
                }
            ]
        )['DBInstances']

        # 각 인스턴스에 대해
        for instance in instances:
            # 인스턴스가 MySQL Aurora인 경우
            if instance['Engine'] == 'aurora' and instance['EngineVersion'].startswith('5.6'):
                instance_info.append({
                    'targets': [f"{instance['Endpoint']['Address']}:{instance['Endpoint']['Port']}"],
                    'labels': {
                        'cluster': cluster_id,
                        'aws': 'awsaccountname',  # 실제 AWS 계정 이름으로 대체해야 합니다.
                        'dbinstance': instance['DBInstanceIdentifier']
                    }
                })

    return instance_info

def update_kubernetes_configmap(instance_info):
    # ConfigMap 데이터 생성
    configmap_data = {
        'mysqld-instances.json': json.dumps(instance_info)
    }

    # kubeconfig 로드
    config.load_kube_config()

    # Kubernetes API 인스턴스 생성
    v1 = client.CoreV1Api()

    # ConfigMap 생성 또는 업데이트
    try:
        # ConfigMap이 이미 존재하는 경우 업데이트
        v1.patch_namespaced_config_map(
            name='mysqld-instances',
            namespace='monitoring',
            body=client.V1ConfigMap(
                api_version='v1',
                kind='ConfigMap',
                data=configmap_data
            )
        )
    except client.exceptions.ApiException as e:
        # ConfigMap이 존재하지 않는 경우 생성
        if e.status == 404:
            v1.create_namespaced_config_map(
                namespace='monitoring',
                body=client.V1ConfigMap(
                    api_version='v1',
                    kind='ConfigMap',
                    metadata=client.V1ObjectMeta(
                        name='mysqld-instances'
                    ),
                    data=configmap_data
                )
            )
        else:
            raise

def main():
    # 관리하고 있는 RDS 클러스터 리스트
    clusters = ['testdb-abcd', 'testdb-edfr']
    instance_info = get_aws_rds_instances(clusters)
    update_kubernetes_configmap(instance_info)

if __name__ == "__main__":
    main()