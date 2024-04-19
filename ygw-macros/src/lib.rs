extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::Parser, parse_macro_input, spanned::Spanned, Field, ItemStruct, LitBool, LitStr, Result,
};

/// parameter_group is a macro that can be used for structs to allow their content to be used as Yamcs parameters
///
/// The macro adds a `_meta` member which stores extra information about each parameter as well as a sequence count 
/// incremented each time data is delivered to Yamcs
/// 
/// To create a value of the struct the meta has to be initialized using the `StructNameMeta::new()` method. This method will allocate numeric identifier for parameters which are then used to communicate the values to/from Yamcs.
/// 
/// In addition, several helper methods are provided:
/// * `send_definitions` - sends the parameter definitions to Yamcs. This has to be called at the beginning, before sending any other data. 
/// * `set_<fieldname>` - can be used to set values of the fields together with the timestamp. This method will set the modified flag of the respective field and that flag is used by the `send_modified_values` function. The fields can also be assigned directly and the send_values method can then be used to send the values to Yamcs.
/// * `send_values` - sends all the values to Yamcs irrespective of their modified status.
/// * `send_modified_values` - sends all the modified values to Yamcs.

#[proc_macro_attribute]
pub fn parameter_group(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let item_struct = parse_macro_input!(input as ItemStruct);

    match parameter_pool_impl(item_struct) {
        Ok(v) => v,
        Err(err) => err.to_compile_error().into(),
    }
}

fn parameter_pool_impl(mut item_struct: ItemStruct) -> Result<TokenStream> {
    let struct_name = &item_struct.ident;

    let mut meta_fields = Vec::new();

    //meta_fields.push(quote!({start_id: u32}));

    let meta_name = format_ident!("{}Meta", struct_name);

    let mut setters = Vec::new();
    let mut meta_inits = Vec::new();
    let mut param_defs = Vec::new();
    let mut param_mod_values = Vec::new();
    let mut param_values = Vec::new();

    let mut field_id = 0;
    let num_fields;

    if let syn::Fields::Named(ref mut fields) = item_struct.fields {
        num_fields = fields.named.len() as u32;
        for f in fields.named.iter_mut() {
            let (extra_meta_fields, init, setter) = get_meta_fields(f)?;
            meta_fields.extend(extra_meta_fields);
            meta_inits.push(init);
            setters.push(setter);

            param_defs.push(get_para_def(field_id, &f)?);
            param_mod_values.push(get_para_mod_value(field_id, &f)?);
            param_values.push(get_para_value(field_id, &f)?);

            f.attrs.clear();
            field_id += 1;
        }
        fields.named.push(
            syn::Field::parse_named
                .parse2(quote!( _meta: #meta_name))
                .unwrap(),
        );
    } else {
        return Err(syn::Error::new_spanned(
            &item_struct,
            "parameter_pool cannot be used with tuple or unit structs",
        ));
    }

    let struct_name_str = format!("{}", struct_name);

    let r = quote! {
        #item_struct


        impl #struct_name {
            #(#setters)*

            fn get_definitions(&self) -> ygw::protobuf::ygw::ParameterDefinitionList {
                let mut definitions = Vec::new();
                #(#param_defs)*

                return ygw::protobuf::ygw::ParameterDefinitionList{ definitions };
            }

            fn get_modified_values(&mut self) -> ygw::protobuf::ygw::ParameterData {
                let mut parameters = Vec::new();
                #(#param_mod_values)*

                ygw::protobuf::ygw::ParameterData {
                    parameters,
                    group: #struct_name_str.to_string(),
                    seq_num: self._meta.next_seq_count(),
                    generation_time: None,
                }
            }

            fn get_values(&mut self, gentime: ygw::protobuf::ygw::Timestamp) -> ygw::protobuf::ygw::ParameterData {
                let mut parameters = Vec::new();
                #(#param_values)*

                ygw::protobuf::ygw::ParameterData {
                    parameters,
                    group: #struct_name_str.to_string(),
                    seq_num: self._meta.next_seq_count(),
                    generation_time: Some(gentime)
                }
            }

            /// sends the definitions
            pub async fn send_definitions(&mut self, tx: &Sender<YgwMessage>) -> ygw::Result<()> {
                tx.send(ygw::msg::YgwMessage::ParameterDefinitions(self._meta.addr, self.get_definitions()))
                   .await.map_err(|_| YgwError::ServerShutdown)
            }

            /// send the values with the current (now) timestamp
            pub async fn send_values(&mut self, tx: &Sender<YgwMessage>) -> ygw::Result<()> {
                 tx.send(ygw::msg::YgwMessage::Parameters(self._meta.addr, self.get_values(ygw::protobuf::now())))
                    .await.map_err(|_| YgwError::ServerShutdown)
            }

            /// send the modified values and clear the modified flag
            pub async fn send_modified_values(&mut self, tx: &Sender<YgwMessage>) -> ygw::Result<()> {
                tx.send(ygw::msg::YgwMessage::Parameters(self._meta.addr, self.get_modified_values()))
                   .await.map_err(|_| YgwError::ServerShutdown)
           }

        }
        struct #meta_name {
            start_id: u32,
            seq_count: u32,
            addr: ygw::msg::Addr,
            #(#meta_fields,)*
        }

        impl #meta_name {
            fn new(addr:ygw::msg::Addr, gentime: ygw::protobuf::ygw::Timestamp) -> Self {
                Self {
                    addr,
                    start_id: ygw::generate_pids(#num_fields),
                    seq_count: 0,
                    #(#meta_inits)*
                }
            }
            fn next_seq_count(&mut self) -> u32 {
                let c = self.seq_count;
                self.seq_count = c + 1;
                c
            }
        }
    }
    .into();

    Ok(r)
}

fn get_para_def(id: u32, field: &Field) -> Result<proc_macro2::TokenStream> {
    let name_string = format!("{}", field.ident.as_ref().unwrap());
    let mut relative_name = quote! {#name_string.to_string()};
    let mut description = quote! { None };
    let mut unit = quote! { None };
    let mut writable = quote! { None };

    for attr in &field.attrs {
        if attr.path().is_ident("mdb") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("relative_name") {
                    let value = meta.value()?;
                    let s: LitStr = value.parse()?;
                    relative_name = quote! {#s.to_string()};
                } else if meta.path.is_ident("description") {
                    let value = meta.value()?;
                    let s: LitStr = value.parse()?;
                    description = quote! {Some(#s.to_string())};
                } else if meta.path.is_ident("unit") {
                    let value = meta.value()?;
                    let s: LitStr = value.parse()?;
                    unit = quote! {Some(#s.to_string())};
                } else if meta.path.is_ident("writable") {
                    let value = meta.value()?;
                    let s: LitBool = value.parse()?;
                    writable = quote! {Some(#s)};
                }
                Ok(())
            })?;
        };
    }
    let types = map_type(&field.ty)?;

    Ok(quote! {
        definitions.push(ParameterDefinition {
            relative_name: #relative_name,
            description: #description,
            unit: #unit,
            writable: #writable,
            ptype: #types.to_string(),
            id: self._meta.start_id + #id
        });
    })
}

fn get_para_value(id: u32, field: &Field) -> Result<proc_macro2::TokenStream> {
    let name = field.ident.as_ref().unwrap();

    Ok(quote! {
        parameters.push(ygw::protobuf::get_pv_eng(self._meta.start_id + #id, None, self.#name));
    })
}

fn get_para_mod_value(id: u32, field: &Field) -> Result<proc_macro2::TokenStream> {
    let name = field.ident.as_ref().unwrap();
    let gentime_name = format_ident!("{}_gentime", name);
    let modified_name = format_ident!("{}_modified", name);

    Ok(quote! {
        if self._meta.#modified_name {
            parameters.push(ygw::protobuf::get_pv_eng(self._meta.start_id + #id, Some(self._meta.#gentime_name.clone()), self.#name));
            self._meta.#modified_name = false;
        }
    })
}

fn get_meta_fields(
    field: &Field,
) -> Result<(
    Vec<Field>,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
)> {
    let name = field.ident.as_ref().unwrap();
    let ty = &field.ty;
    let setter_name = format_ident!("set_{}", name);

    let gentime_name = format_ident!("{}_gentime", name);
    let modified_name = format_ident!("{}_modified", name);

    let gentime_field = syn::Field::parse_named
        .parse2(quote! { #gentime_name: ygw::protobuf::ygw::Timestamp })
        .unwrap();
    let modified_field = syn::Field::parse_named
        .parse2(quote! { #modified_name: bool })
        .unwrap();

    let setter = quote! {
        pub fn #setter_name(&mut self, #name: #ty, gentime: ygw::protobuf::ygw::Timestamp) {
        self.#name = #name;
        self._meta.#gentime_name = gentime;
        self._meta.#modified_name = true;
    }};

    let init = quote! {
        #gentime_name: gentime.clone(),
        #modified_name: false,
    };

    Ok((vec![gentime_field, modified_field], init, setter))
}

fn map_type(ty: &syn::Type) -> Result<String> {
    match ty {
        syn::Type::Path(typepath) if typepath.qself.is_none() => {
            let type_ident = &typepath.path.segments.first().unwrap().ident;

            match type_ident.to_string().as_ref() {
                "u32" => Ok("uint32".to_string()),
                "i32" => Ok("sint32".to_string()),
                "u64" => Ok("uint64".to_string()),
                "i64" => Ok("sint64".to_string()),
                "f32" => Ok("float".to_string()),
                "f64" => Ok("double".to_string()),
                "bool" => Ok("boolean".to_string()),
                "String" => Ok("string".to_string()),
                _ => Err(syn::Error::new(type_ident.span(), "Invalid Type")),
            }
        }
        _ => Err(syn::Error::new(ty.span(), "Not a simple type")),
    }
}
