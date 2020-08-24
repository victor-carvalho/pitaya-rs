use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{
    parse::Parser, parse_macro_input, punctuated::Punctuated, Expr, ExprCall, ExprMethodCall,
    ItemFn, LitStr, Path, Token,
};

fn get_handler_info_name(method_name: &str) -> Ident {
    Ident::new(
        &format!("pitaya_handler_info_{}", method_name).to_uppercase(),
        Span::call_site(),
    )
}

#[proc_macro]
pub fn handlers(item: TokenStream) -> TokenStream {
    let mut paths = match <Punctuated<Path, Token![,]>>::parse_terminated.parse(item) {
        Ok(p) => p,
        Err(_e) => {
            return TokenStream::from(quote! {
                compiler_error!("expecting a comma separated list of handler functions");
            });
        }
    };

    let mut punct: Punctuated<ExprMethodCall, syn::Token![;]> = Punctuated::new();

    paths.iter_mut().for_each(|p| {
        let seg = p.segments.last().expect("should have segments");
        let handler_info_name = get_handler_info_name(&seg.ident.to_string());
        let call = ExprMethodCall {
            attrs: vec![],
            receiver: Box::new(Expr::Verbatim(quote! { temp_handlers })),
            dot_token: syn::token::Dot {
                spans: [Span::call_site()],
            },
            method: Ident::new("add", Span::call_site()),
            turbofish: None,
            paren_token: syn::token::Paren {
                span: Span::call_site(),
            },
            args: {
                let mut args = Punctuated::new();
                args.push_value(Expr::Verbatim(quote! { &#handler_info_name }));
                args
            },
        };

        punct.push_value(call);
        punct.push_punct(syn::token::Semi {
            spans: [Span::call_site()],
        });
    });

    TokenStream::from(quote! {
        {
            let mut temp_handlers = ::pitaya_core::handler::Handlers::new();
            #punct
            temp_handlers
        }
    })
}

#[proc_macro_attribute]
pub fn json_handler(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as ItemFn);

    let attrs = proc_macro2::TokenStream::from(attrs);
    let handler_name_lit = match syn::parse2::<LitStr>(attrs) {
        Ok(l) => l,
        Err(_e) => {
            return quote! {
                compile_error!("can only receive strings inside attribute invocation");
            }
            .into()
        }
    };

    let method_ident = item.sig.ident.clone();
    let method_name = method_ident.to_string();
    let method_name_lit = LitStr::new(&method_name, Span::call_site());

    let generated_handler = Ident::new(
        &format!("pitaya_handler_{}", &method_name),
        Span::call_site(),
    );

    let handler_info = get_handler_info_name(&method_name);

    let func_call = ExprCall {
        attrs: vec![],
        func: Box::new(Expr::Verbatim(quote! { #method_ident })),
        paren_token: syn::token::Paren {
            span: Span::call_site(),
        },
        args: Punctuated::new(),
    };

    let has_return = match item.sig.output {
        syn::ReturnType::Default => false,
        syn::ReturnType::Type(_, ref ty) => {
            if let syn::Type::Tuple(ref tuple) = ty.as_ref() {
                !tuple.elems.is_empty()
            } else {
                true
            }
        }
    };

    if !has_return {
        return quote! { compile_error!("json handlers must return Result<T, E>"); }.into();
    }

    let body = quote! {
        use ::pitaya_core::ToResponseJson;
        let fut = async {
            let res = #func_call.await;
            res.to_response_json()
        };
        ::std::boxed::Box::pin(fut)
    };

    let output = quote! {
        #item

        const #handler_info: ::pitaya_core::handler::StaticHandlerInfo = ::pitaya_core::handler::StaticHandlerInfo {
            handler_name: #handler_name_lit,
            method_name: #method_name_lit,
            method: #generated_handler,
        };

        fn #generated_handler(
            ctx: ::pitaya_core::context::Context,
            req: &::pitaya_core::protos::Request,
        ) -> ::std::pin::Pin<::std::boxed::Box<dyn ::std::future::Future<Output = ::pitaya_core::protos::Response> + Send +'static>> {
            #body
        }
    };

    TokenStream::from(output)
}
