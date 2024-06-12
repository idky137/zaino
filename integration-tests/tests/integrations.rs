//! Integration tests for zingo-Proxy.
//! Currently uses ZCashD as ZebraD has not yet implemented Regtest Mode.

#![forbid(unsafe_code)]

use std::sync::{atomic::AtomicBool, Arc};
use zingoproxy_testutils::{drop_test_manager, TestManager};

use zingo_netutils::GrpcConnector;

mod wallet {
    use super::*;

    #[tokio::test]
    async fn connect_to_node_get_info() {
        let online = Arc::new(AtomicBool::new(true));
        let (test_manager, regtest_handler, _proxy_handler) =
            TestManager::launch(online.clone()).await;

        println!(
            "@zingoproxytest: Attempting to connect to GRPC server at URI: {}.",
            test_manager.get_proxy_uri()
        );
        let mut client = GrpcConnector::new(test_manager.get_proxy_uri())
            .get_client()
            .await
            .expect("Failed to create GRPC client");
        let lightd_info = client
            .get_lightd_info(zcash_client_backend::proto::service::Empty {})
            .await
            .expect("Failed to retrieve lightd info from GRPC server");

        println!(
            "@zingoproxytest: Lightd_info response:\n{:#?}.",
            lightd_info.into_inner()
        );
        drop_test_manager(
            Some(test_manager.temp_conf_dir.path().to_path_buf()),
            regtest_handler,
            online,
        )
        .await;
    }

    #[tokio::test]
    async fn send_and_sync_shielded() {
        let online = Arc::new(AtomicBool::new(true));
        let (test_manager, regtest_handler, _proxy_handler) =
            TestManager::launch(online.clone()).await;
        let zingo_client = test_manager.build_lightclient().await;

        test_manager.regtest_manager.generate_n_blocks(1).unwrap();
        zingo_client.do_sync(false).await.unwrap();

        // std::thread::sleep(std::time::Duration::from_secs(10));

        zingo_client
            .do_send(vec![(
                &zingolib::get_base_address!(zingo_client, "sapling"),
                250_000,
                None,
            )])
            .await
            .unwrap();
        test_manager.regtest_manager.generate_n_blocks(1).unwrap();
        zingo_client.do_sync(false).await.unwrap();
        let balance = zingo_client.do_balance().await;
        println!("@zingoproxytest: zingo_client balance: \n{:#?}.", balance);

        assert_eq!(balance.sapling_balance.unwrap(), 250_000);
        drop_test_manager(
            Some(test_manager.temp_conf_dir.path().to_path_buf()),
            regtest_handler,
            online,
        )
        .await;
    }

    #[tokio::test]
    async fn send_and_sync_transparent() {
        let online = Arc::new(AtomicBool::new(true));
        let (test_manager, regtest_handler, _proxy_handler) =
            TestManager::launch(online.clone()).await;
        let zingo_client = test_manager.build_lightclient().await;

        test_manager.regtest_manager.generate_n_blocks(1).unwrap();
        zingo_client.do_sync(false).await.unwrap();
        zingo_client
            .do_send(vec![(
                &zingolib::get_base_address!(zingo_client, "transparent"),
                250_000,
                None,
            )])
            .await
            .unwrap();
        zingo_client
            .do_send(vec![(
                &zingolib::get_base_address!(zingo_client, "transparent"),
                250_000,
                None,
            )])
            .await
            .unwrap();
        test_manager.regtest_manager.generate_n_blocks(1).unwrap();
        zingo_client.do_sync(false).await.unwrap();
        let balance = zingo_client.do_balance().await;
        println!("@zingoproxytest: zingo_client balance: \n{:#?}.", balance);

        assert_eq!(balance.transparent_balance.unwrap(), 500_000);
        drop_test_manager(
            Some(test_manager.temp_conf_dir.path().to_path_buf()),
            regtest_handler,
            online,
        )
        .await;
    }

    // TODO: Add test for get_mempool_stream: lightclient::start_mempool_monitor.
    // #[tokio::test]
    // async fn mempool_monitor() {}
}

mod nym {
    // TODO: Build nym enhanced zingolib version using zingo-rpc::walletrpc::service.
}

mod darkside {
    // TODO: Add darkside.
}
