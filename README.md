Yamcs Gateway

The goal of this project is to allow Yamcs to control instruments/payloads as part of an EGSE. 

It is implemented in Rust to allow close to realtime behavior. The connection to Yamcs is via TCP.

As seen in the diagram below the program is composed of an async part on top of tokio interacting with a set of components (called ``ygw nodes``) that implement the communication with the hardware devices. The nodes may chose to be sync or async and the sync ones can run into their own thread set to realtime priority if required. Communication between the async and sync nodes is done using Tokio channels.

![ ALT](/drawings/yamcs-gateway.drawio.png)

Each node (corresponding to an end device) gets assigned a name and a u32 id. The names and ids are communicated to Yamcs which will use them to route commands and parameters.

This crate (yamcs-if) contains the code for communication with Yamcs and several standard nodes:
 - UDP TM - receives TM packets via UDP and passes them to Yamcs
 - UDP TC - passes TC from Yamcs via UDP to a remote device.

The create does not contain an executable. Each project should setup a separate binary executable crate combining the components from this crate possibly with its own components.


**Target implementation and usage**
To create a target, each implementation will generally provide a method

```rust
async fn new(target_id, config) -> Result<Target> {

}
``` 

The method will implement initialization relevant for the specific target. It may return an error for example if failing to open a hardware device, a network connection, etc.

The returned target contains the following fields:
* target_id - this is the id of the target used in all internal messages
* name -  a friendly but short name for the target. Yamcs uses it to construct the name of the links to/from the target.
* description - a more elaborate description of the target. In the future it may show in the Yamcs web-ui somewhere - TBD.
* tx: Sender<YgwMessage> - used by the Yamcs to send TC/Parameters and control messages to the target. First control message is TargetInit(Sender<YgwMessage>). This contains the channel to be used by the target to send data (TM, event, parameters) back to Yamcs.
* TODO: configuration information to know if the target supports TM, TC, Parameters, etc

The code that implements the target has to wait for the first TargetInit message to start operations.

