use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{Ident, ItemFn};

/// Marks a function as a simulation test
///
/// This is a simple marker attribute that doesn't transform the code.
/// The external test runner discovers these functions by parsing the AST
/// and looking for this attribute.
#[proc_macro_attribute]
pub fn sim(_args: TokenStream, input: TokenStream) -> TokenStream {
    // Parse and validate the function signature
    let input_fn = syn::parse_macro_input!(input as syn::ItemFn);

    // Validate it's async and returns Result<SimulationBuilder<_>>
    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(input_fn.sig.fn_token, "sim functions must be async")
            .to_compile_error()
            .into();
    }

    expand_sim_fn(input_fn)
}

fn expand_sim_fn(input_fn: ItemFn) -> TokenStream {
    let name = input_fn.sig.ident.clone();
    let name_str = name.to_string();
    let new_name = Ident::new(&format!("n0des_sim_{name}"), Span::call_site());
    quote! {
        #[tokio::test]
        async fn #new_name() -> ::anyhow::Result<()> {
            #input_fn

            ::iroh_n0des::simulation::run_sim_fn(#name_str, #name).await
        }
    }
    .into()
}
