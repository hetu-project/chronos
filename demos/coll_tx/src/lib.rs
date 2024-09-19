//! The coll-tx demo is about collecting simple utxo transaction with cops and vlc.

pub mod simple_utxo;

#[cfg(test)]
mod tests {
    use cops::{Configuration, Server, ServerState, Client};
    use tokio::task::JoinHandle;

    use crate::simple_utxo::{TxInput, TxOutput, SimpleUTXO};

    async fn start_servers(n_server: usize) -> (Configuration, Vec<JoinHandle<ServerState>>) {
        let mut config = Configuration {
            server_addrs: Vec::new(),
        };
        let port = 8000 + (rand::random::<usize>() % 100) * 10;
        for i in 0..n_server {
            let addr = format!("127.0.0.1:{}", port + i).parse().unwrap();
            config.server_addrs.push(addr);
        }
        let mut handles = Vec::new();
        for i in 0..n_server {
            let c = config.clone();
            handles.push(tokio::spawn(async move {
                let mut server = Server::new(&c, i).await;
                server.run().await;
                server.get_server_state().clone()
            }));
        }
        (config, handles)
    }

    async fn collect_states(handles: Vec<JoinHandle<ServerState>>) -> Vec<ServerState> {
        let mut states = Vec::new();
        for handle in handles {
            states.push(handle.await.unwrap());
        }
        states
    }

    async fn terminate(config: &Configuration) {
        let mut client = Client::new(config).await;
        for i in 0..config.server_addrs.len() {
            client.terminate(i).await;
        }
    }

    #[tokio::test]
    async fn utxo_tx_in_same_server() {
        // Start server
        let (config, handles) = start_servers(1).await;
        // Run client
        let mut client = Client::new(&config).await;

        let tin = TxInput::new(vec![116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 115, 116, 114, 105, 110, 103], 0, vec![4,5,6]);
        let tout = TxOutput::new(100, vec![7, 8, 9]);
        let tutxo = SimpleUTXO::new_with_inputs_outputs(vec![tin], vec![tout]);

        let vec_id = tutxo.tx_id();
        let vec_raw_tx = tutxo.encode();
        let txid = std::str::from_utf8(&vec_id).unwrap_or("Invalid UTF-8 sequence");
        let raw_tx = std::str::from_utf8(&vec_raw_tx).unwrap_or("Invalid UTF-8 sequence");
        
        println!("txid is {}", txid);
        client.write(txid, raw_tx, Some(0)).await;
       
        assert_eq!(
            client.read(txid, Some(0)).await,
            (true, Some(raw_tx.to_string()))
        );
        
        terminate(&config).await;
        let mut states = collect_states(handles).await;
        assert_eq!(states[0].read(txid), Some(raw_tx.to_string()));
    }
}